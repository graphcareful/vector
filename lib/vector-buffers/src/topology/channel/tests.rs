use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::{pin, sync::Barrier, time::sleep};

use crate::{
    Bufferable, WhenFull,
    topology::{
        channel::{BufferReceiver, BufferSender},
        test_util::{assert_current_send_capacity, build_buffer},
    },
};

async fn assert_send_ok_with_capacities<T>(
    sender: &mut BufferSender<T>,
    value: impl Into<T>,
    base_expected: Option<usize>,
) where
    T: Bufferable,
{
    assert!(sender.send(value.into(), None).await.is_ok());
    assert_current_send_capacity(sender, base_expected);
}

async fn blocking_send_and_drain_receiver<T, V>(
    mut sender: BufferSender<T>,
    receiver: BufferReceiver<T>,
    send_value: V,
) -> Vec<V>
where
    T: Bufferable,
    V: Into<T> + From<T> + Send + 'static,
{
    // We can likely replace this with `tokio_test`-related helpers to avoid the sleeping.
    let send_baton = Arc::new(Barrier::new(2));
    let recv_baton = Arc::clone(&send_baton);
    let recv_delay = Duration::from_millis(500);
    let handle = tokio::spawn(async move {
        let mut results = Vec::new();
        pin!(receiver);

        // Synchronize with sender and then wait for a small period of time to simulate a
        // blocking delay.
        _ = recv_baton.wait().await;
        sleep(recv_delay).await;

        // Grab all messages and then return the results.
        while let Some(msg) = receiver.next().await {
            results.push(msg.into());
        }
        results
    });

    // We also have to drop our sender after sending the fourth message so that the receiver
    // task correctly exits.  If we didn't drop it, the receiver task would just assume that we
    // had no more messages to send, waiting for-ev-er for the next one.
    let start = Instant::now();
    _ = send_baton.wait().await;
    assert!(sender.send(send_value.into(), None).await.is_ok());
    let send_delay = start.elapsed();
    assert!(send_delay > recv_delay);
    drop(sender);

    handle.await.expect("receiver task should not panic")
}

async fn drain_receiver<T, V>(sender: BufferSender<T>, receiver: BufferReceiver<T>) -> Vec<V>
where
    T: Bufferable,
    V: From<T> + Send + 'static,
{
    drop(sender);
    let handle = tokio::spawn(async move {
        let mut results = Vec::new();
        pin!(receiver);

        // Grab all messages and then return the results.
        while let Some(msg) = receiver.next().await {
            results.push(msg.into());
        }
        results
    });

    handle.await.expect("receiver task should not panic")
}

#[tokio::test]
async fn test_sender_block() {
    // Get a buffer in blocking mode with a capacity of 3.
    let (mut tx, rx, _) = build_buffer(3, WhenFull::Block);

    // We should be able to send three messages through unimpeded.
    assert_current_send_capacity(&mut tx, Some(3));
    assert_send_ok_with_capacities(&mut tx, 1, Some(2)).await;
    assert_send_ok_with_capacities(&mut tx, 2, Some(1)).await;
    assert_send_ok_with_capacities(&mut tx, 3, Some(0)).await;

    // Our next send _should_ block.  `assert_sender_blocking_send_and_recv` spawns a receiver
    // task which waits for a small period of time, and we track how long our next send blocks
    // for, which should be greater than the time that the receiver task waits.  This asserts
    // that the send is blocking, and that it's dependent on the receiver.
    //
    // It also drops the sender and receives all remaining messages on the receiver, returning
    // them to us to check.
    let mut results = blocking_send_and_drain_receiver(tx, rx, 4).await;
    results.sort_unstable();
    assert_eq!(results, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn test_sender_drop_newest() {
    // Get a buffer in "drop newest" mode with a capacity of 3.
    let (mut tx, rx, _) = build_buffer(3, WhenFull::DropNewest);

    // We should be able to send three messages through unimpeded.
    assert_current_send_capacity(&mut tx, Some(3));
    assert_send_ok_with_capacities(&mut tx, 1, Some(2)).await;
    assert_send_ok_with_capacities(&mut tx, 2, Some(1)).await;
    assert_send_ok_with_capacities(&mut tx, 3, Some(0)).await;

    // Then, since we're in "drop newest" mode, we could continue to send without issue or being
    // blocked, but we would except those items to, well.... be dropped.
    assert_send_ok_with_capacities(&mut tx, 7, Some(0)).await;
    assert_send_ok_with_capacities(&mut tx, 8, Some(0)).await;
    assert_send_ok_with_capacities(&mut tx, 9, Some(0)).await;

    // Then, when we collect all of the messages from the receiver, we should only get back the
    // first three of them.
    let mut results: Vec<u64> = drain_receiver(tx, rx).await;
    results.sort_unstable();
    assert_eq!(results, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_buffer_metrics_normal() {
    // Get a regular blocking buffer.
    let (mut tx, rx, handle) = build_buffer(5, WhenFull::Block);

    // Send three items through, and make sure the buffer usage stats reflect that.
    assert_current_send_capacity(&mut tx, Some(5));
    assert_send_ok_with_capacities(&mut tx, 7, Some(4)).await;
    assert_send_ok_with_capacities(&mut tx, 8, Some(3)).await;
    assert_send_ok_with_capacities(&mut tx, 2, Some(2)).await;

    let snapshot = handle.snapshot();
    assert_eq!(3, snapshot.received_event_count);
    assert_eq!(0, snapshot.sent_event_count);
    assert_eq!(0, snapshot.dropped_event_count_intentional);

    // Then, when we collect all of the messages from the receiver, the metrics should also reflect that.
    let mut results: Vec<u64> = drain_receiver(tx, rx).await;
    results.sort_unstable();
    assert_eq!(results, vec![2, 7, 8]);

    let snapshot = handle.snapshot();
    assert_eq!(3, snapshot.received_event_count);
    assert_eq!(3, snapshot.sent_event_count);
    assert_eq!(0, snapshot.dropped_event_count_intentional);
}

#[tokio::test]
async fn test_buffer_metrics_drop_newest() {
    // Get a buffer that drops the newest items when full.
    let (mut tx, rx, handle) = build_buffer(2, WhenFull::DropNewest);

    // Send three items through, and make sure the buffer usage stats reflect that.
    assert_current_send_capacity(&mut tx, Some(2));
    assert_send_ok_with_capacities(&mut tx, 7, Some(1)).await;
    assert_send_ok_with_capacities(&mut tx, 8, Some(0)).await;
    assert_send_ok_with_capacities(&mut tx, 2, Some(0)).await;

    let snapshot = handle.snapshot();
    assert_eq!(3, snapshot.received_event_count);
    assert_eq!(0, snapshot.sent_event_count);
    assert_eq!(1, snapshot.dropped_event_count_intentional);

    // Then, when we collect all of the messages from the receiver, the metrics should also reflect that.
    let mut results: Vec<u64> = drain_receiver(tx, rx).await;
    results.sort_unstable();
    assert_eq!(results, vec![7, 8]);

    let snapshot = handle.snapshot();
    assert_eq!(3, snapshot.received_event_count);
    assert_eq!(2, snapshot.sent_event_count);
    assert_eq!(1, snapshot.dropped_event_count_intentional);
}
