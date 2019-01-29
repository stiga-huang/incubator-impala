// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.catalog.events;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.metastore.messaging.json.ExtendedJSONMessageFactory;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEvent;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventFactory;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

/**
 * A metastore event is a instance of the class
 * <code>org.apache.hadoop.hive.metastore.api.NotificationEvent</code>. Metastore can be
 * configured, to work with Listeners which are called on various DDL operations like
 * create/alter/drop operations on database, table, partition etc. Each event has a unique
 * incremental id and the generated events are be fetched from Metastore to get
 * incremental updates to the metadata stored in Hive metastore using the the public API
 * <code>get_next_notification</code> These events could be generated by external
 * Metastore clients like Apache Hive or Apache Spark as well as other Impala clusters
 * configured to talk with the same metastore.
 *
 * This class is used to poll metastore for such events at a given frequency. By observing
 * such events, we can take appropriate action on the catalogD (invalidate/add/remove) so
 * that catalog represents the latest information available in metastore. We keep track of
 * the last synced event id in each polling iteration so the next batch can be requested
 * appropriately. The current batch size is constant and set to MAX_EVENTS_PER_RPC.
 *
 * <pre>
 *      +---------------+   +----------------+        +--------------+
 *      |Catalog state  |   |Catalog         |        |              |
 *      |stale          |   |State up-to-date|        |Catalog state |
 *      |(apply event)  |   |(ignore)        |        |is newer than |
 *      |               |   |                |        |event         |
 *      |               |   |                |        |(ignore)      |
 *      +------+--------+   +-----+----------+        +-+------------+
 *             |                  |                     |
 *             |                  |                     |
 *             |                  |                     |
 *             |                  |                     |
 *             |                  |                     |
 * +-----------V------------------V---------------------V----------->  Event Timeline
 *                                ^
 *                                |
 *                                |
 *                                |
 *                                |
 *                                E
 *
 * </pre>
 * Consistency model: Events could be seen as DDLs operations from past either done from
 * this same cluster or some other external system. For example in the events timeline
 * given above, consider a Event E at any given time. The catalog state for the
 * corresponding object of the event could either be stale, exactly-same or at a version
 * which is higher than one provided by event. Catalog state should only be updated when
 * it is stale with respect to the event. In order to determine if the catalog object is
 * stale, we rely on a combination of creationTime and object version. A object in catalog
 * is stale if and only if its creationTime is < creationTime of the object from event OR
 * its version < version from event if createTime matches
 *
 * If the object has the same createTime and version when compared to event or if the
 * createTime > createTime from the event, the event can be safely ignored.
 *
 * Following table shows the actions to be taken when the catalog state is stale.
 *
 * <pre>
 *               +----------------------------------------+
 *               |    Catalog object state                |
 * +----------------------------+------------+------------+
 * | Event type  | Loaded       | Incomplete | Not present|
 * |             |              |            |            |
 * +------------------------------------------------------+
 * |             |              |            |            |
 * | CREATE EVENT| removeAndAdd | Ignore     | Add        |
 * |             |              |            |            |
 * |             |              |            |            |
 * | ALTER EVENT | Invalidate   | Ignore     | Ignore     |
 * |             |              |            |            |
 * |             |              |            |            |
 * | DROP EVENT  | Remove       | Remove     | Ignore     |
 * |             |              |            |            |
 * +-------------+--------------+------------+------------+
 * </pre>
 *
 * //TODO - Object version support is a work-in-progress in Hive (HIVE-21115). Current
 * event handlers only rely on createTime on Table and Partition. Database createTime is a
 * work-in-progress in Hive in (HIVE-20776)
 *
 * All the operations which change the state of catalog cache while processing a certain
 * event type must be atomic in nature. We rely on taking a write lock on version object
 * in CatalogServiceCatalog to make sure that readers are blocked while the metadata
 * update operation is being performed. Since the events are generated post-metastore
 * operations, such catalog updates do not need to update the state in Hive Metastore.
 *
 * Error Handling: The event processor could be in ACTIVE, STOPPED, ERROR states. In case
 * of any errors while processing the events the state of event processor changes to ERROR
 * and no subsequent events are polled. In such a case a invalidate metadata command
 * restarts the event polling which updates the lastSyncedEventId to the latest from
 * metastore.
 */
public class MetastoreEventsProcessor implements ExternalEventsProcessor {

  public static final String HMS_ADD_THRIFT_OBJECTS_IN_EVENTS_CONFIG_KEY =
      "hive.metastore.notifications.add.thrift.objects";
  private static final Logger LOG = Logger.getLogger(MetastoreEventsProcessor.class);
  // Use ExtendedJSONMessageFactory to deserialize the event messages.
  // ExtendedJSONMessageFactory adds additional information over JSONMessageFactory so
  // that events are compatible with Sentry
  // TODO this should be moved to JSONMessageFactory when Sentry switches to
  // JSONMessageFactory
  private static final MessageFactory messageFactory =
      ExtendedJSONMessageFactory.getInstance();

  private static MetastoreEventsProcessor instance;

  // maximum number of events to poll in each RPC
  private static final int EVENTS_BATCH_SIZE_PER_RPC = 1000;

  // possible status of event processor
  public enum EventProcessorStatus {
    STOPPED, // event processor is instantiated but not yet scheduled
    ACTIVE, // event processor is scheduled at a given frequency
    ERROR, // event processor is in error state and event processing has stopped
    NEEDS_INVALIDATE // event processor could not resolve certain events and needs a
    // manual invalidate command to reset the state (See AlterEvent for a example)
  }

  // current status of this event processor
  private EventProcessorStatus eventProcessorStatus_ = EventProcessorStatus.STOPPED;

  // event factory which is used to get or create MetastoreEvents
  private final MetastoreEventFactory metastoreEventFactory_;

  // keeps track of the last event id which we have synced to
  private long lastSyncedEventId_;

  // polling interval in seconds. Note this is a time we wait AFTER each fetch call
  private final long pollingFrequencyInSec_;

  // catalog service instance to be used while processing events
  private final CatalogServiceCatalog catalog_;

  // scheduler daemon thread executor for processing events at given frequency
  private final ScheduledExecutorService scheduler_ = Executors
      .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true)
          .setNameFormat("MetastoreEventsProcessor").build());

  @VisibleForTesting
  MetastoreEventsProcessor(CatalogServiceCatalog catalog, long startSyncFromId,
      long pollingFrequencyInSec) {
    Preconditions.checkState(pollingFrequencyInSec > 0);
    this.catalog_ = Preconditions.checkNotNull(catalog);
    lastSyncedEventId_ = startSyncFromId;
    metastoreEventFactory_ = new MetastoreEventFactory(catalog_);
    pollingFrequencyInSec_ = pollingFrequencyInSec;
  }

  /**
   * Schedules the daemon thread at a given frequency. It is important to note that this
   * method schedules with FixedDelay instead of FixedRate. The reason it is scheduled at
   * a fixedDelay is to make sure that we don't pile up the pending tasks in case each
   * polling operation is taking longer than the given frequency. Because of the fixed
   * delay, the new poll operation is scheduled at the time when previousPoll operation
   * completes + givenDelayInSec
   */
  @Override
  public synchronized void start() {
    Preconditions.checkState(eventProcessorStatus_ != EventProcessorStatus.ACTIVE);
    startScheduler();
    eventProcessorStatus_ = EventProcessorStatus.ACTIVE;
    LOG.info(String.format("Successfully started metastore event processing."
        + " Polling interval: %d seconds.", pollingFrequencyInSec_));
  }

  /**
   * Gets the current event processor status
   */
  @VisibleForTesting
  EventProcessorStatus getStatus() {
    return eventProcessorStatus_;
  }

  /**
   * returns the current value of LastSyncedEventId. This method is not thread-safe and
   * only to be used for testing purposes
   */
  @VisibleForTesting
  public long getLastSyncedEventId() {
    return lastSyncedEventId_;
  }

  @VisibleForTesting
  void startScheduler() {
    Preconditions.checkState(pollingFrequencyInSec_ > 0);
    LOG.info(String.format("Starting metastore event polling with interval %d seconds.",
        pollingFrequencyInSec_));
    scheduler_.scheduleWithFixedDelay(this::processEvents, pollingFrequencyInSec_,
        pollingFrequencyInSec_, TimeUnit.SECONDS);
  }

  /**
   * Stops the event processing and changes the status of event processor to
   * <code>EventProcessorStatus.STOPPED</code>. No new events will be processed as long
   * the status is stopped. If this event processor is actively processing events when
   * stop is called, this method blocks until the current processing is complete
   */
  @Override
  public synchronized void stop() {
    Preconditions.checkState(eventProcessorStatus_ != EventProcessorStatus.STOPPED);
    eventProcessorStatus_ = EventProcessorStatus.STOPPED;
    LOG.info(String.format("Event processing is stopped. Last synced event id is %d",
        lastSyncedEventId_));
  }

  /**
   * Get the current notification event id from metastore
   */
  @Override
  public long getCurrentEventId() throws CatalogException {
    try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
      return metaStoreClient.getHiveClient().getCurrentNotificationEventId().getEventId();
    } catch (TException e) {
      throw new CatalogException("Unable to fetch the current notification event id. "
          + "Check if metastore service is accessible");
    }
  }

  /**
   * Starts the event processor from a given event id
   */
  @Override
  public synchronized void start(long fromEventId) {
    Preconditions.checkArgument(fromEventId >= 0);
    Preconditions.checkState(eventProcessorStatus_ != EventProcessorStatus.ACTIVE,
        "Event processing start called when it is already active");
    long prevLastSyncedEventId = lastSyncedEventId_;
    lastSyncedEventId_ = fromEventId;
    eventProcessorStatus_ = EventProcessorStatus.ACTIVE;
    LOG.info(String.format(
        "Metastore event processing restarted. Last synced event id was updated "
            + "from %d to %d", prevLastSyncedEventId, lastSyncedEventId_));
  }

  /**
   * Fetch the next batch of NotificationEvents from metastore. The default batch size if
   * <code>EVENTS_BATCH_SIZE_PER_RPC</code>
   */
  @VisibleForTesting
  protected List<NotificationEvent> getNextMetastoreEvents()
      throws MetastoreNotificationFetchException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      // fetch the current notification event id. We assume that the polling interval
      // is small enough that most of these polling operations result in zero new
      // events. In such a case, fetching current notification event id is much faster
      // (and cheaper on HMS side) instead of polling for events directly
      CurrentNotificationEventId currentNotificationEventId =
          msClient.getHiveClient().getCurrentNotificationEventId();
      long currentEventId = currentNotificationEventId.getEventId();

      // no new events since we last polled
      if (currentEventId <= lastSyncedEventId_) {
        return Collections.emptyList();
      }

      NotificationEventResponse response = msClient.getHiveClient()
          .getNextNotification(lastSyncedEventId_, EVENTS_BATCH_SIZE_PER_RPC, null);
      LOG.info(String
          .format("Received %d events. Start event id : %d", response.getEvents().size(),
              lastSyncedEventId_));
      return response.getEvents();
    } catch (TException e) {
      throw new MetastoreNotificationFetchException(
          "Unable to fetch notifications from metastore. Last synced event id is "
              + lastSyncedEventId_, e);
    }
  }

  /**
   * This method issues a request to Hive Metastore if needed, based on the current event
   * id in metastore and the last synced event_id. Events are fetched in fixed sized
   * batches. Each NotificationEvent received is processed by its corresponding
   * <code>MetastoreEvent</code>
   */
  @Override
  public void processEvents() {
    NotificationEvent lastProcessedEvent = null;
    try {
      EventProcessorStatus currentStatus = eventProcessorStatus_;
      if (currentStatus != EventProcessorStatus.ACTIVE) {
        LOG.warn(String.format(
            "Event processing is skipped since status is %s. Last synced event id is %d",
            currentStatus, lastSyncedEventId_));
        return;
      }

      List<NotificationEvent> events = getNextMetastoreEvents();
      processEvents(events);
    } catch (MetastoreNotificationFetchException fetchEx) {
      updateStatus(EventProcessorStatus.ERROR);
      LOG.error("Unable to fetch the next batch of metastore events", fetchEx);
    } catch(MetastoreNotificationNeedsInvalidateException ex) {
      updateStatus(EventProcessorStatus.NEEDS_INVALIDATE);
      LOG.error("Event processing needs a invalidate command to resolve the state", ex);
    } catch(MetastoreNotificationException ex) {
      updateStatus(EventProcessorStatus.ERROR);
      LOG.error("Unexpected exception received while processing event", ex);
      dumpEventInfoToLog(lastProcessedEvent);
    }
  }

  /**
   * Process the given list of notification events. Useful for tests which provide a list
   * of events
   *
   * @return the last Notification event which was processed.
   */
  @VisibleForTesting
  protected void processEvents(List<NotificationEvent> events)
      throws MetastoreNotificationException {
    List<MetastoreEvent> filteredEvents =
        metastoreEventFactory_.getFilteredEvents(events);
    NotificationEvent lastProcessedEvent = null;
    try {
      for (MetastoreEvent event : filteredEvents) {
        // synchronizing each event processing reduces the scope of the lock so the a
        // potential reset() during event processing is not blocked for longer than
        // necessary
        synchronized (this) {
          if (eventProcessorStatus_ != EventProcessorStatus.ACTIVE) {
            break;
          }
          lastProcessedEvent = event.metastoreNotificationEvent_;
          event.processIfEnabled();
          lastSyncedEventId_ = event.eventId_;
        }
      }
    } catch (CatalogException e) {
      throw new MetastoreNotificationException(String.format(
          "Unable to process event %d of type %s. "
              + "Event processing will be stopped.", lastProcessedEvent.getEventId(),
          lastProcessedEvent.getEventType()), e);
    }
  }

  /**
   * Updates the current states to the given status.
   */
  private synchronized void updateStatus(EventProcessorStatus toStatus) {
    eventProcessorStatus_ = toStatus;
  }

  private void dumpEventInfoToLog(NotificationEvent event) {
    StringBuilder msg =
        new StringBuilder().append("Event id: ").append(event.getEventId()).append("\n")
            .append("Event Type: ").append(event.getEventType()).append("\n")
            .append("Event time: ").append(event.getEventTime()).append("\n")
            .append("Database name: ").append(event.getDbName()).append("\n");
    if (event.getTableName() != null) {
      msg.append("Table name: ").append(event.getTableName()).append("\n");
    }
    msg.append("Event message: ").append(event.getMessage()).append("\n");
    LOG.error(msg.toString());
  }

  /**
   * Create a instance of this object if it is not initialized. Currently, this object is
   * a singleton and should only be created during catalogD initialization time, so that
   * the start syncId matches with the catalogD startup time.
   *
   * @param catalog the CatalogServiceCatalog instance to which this event processing
   *     belongs
   * @param startSyncFromId Start event id. Events will be polled starting from this
   *     event id
   * @param eventPollingInterval HMS polling interval in seconds
   * @return this object is already created, or create a new one if it is not yet
   *     instantiated
   */
  public static synchronized ExternalEventsProcessor getInstance(
      CatalogServiceCatalog catalog, long startSyncFromId, long eventPollingInterval) {
    if (instance != null) {
      return instance;
    }

    instance =
        new MetastoreEventsProcessor(catalog, startSyncFromId, eventPollingInterval);
    return instance;
  }

  @VisibleForTesting
  public MetastoreEventFactory getMetastoreEventFactory() {
    return metastoreEventFactory_;
  }

  public static MessageFactory getMessageFactory() {
    return messageFactory;
  }
}
