import pytest
import asyncio
import os
from snerdmq import SnerdQueue

# Point to the actual compiled Rust binary in the sibling repository for tests
BIN_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'snerdmq', 'target', 'debug', 'snerdmq'))
DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.snerdata', 'tasks', 'tasks.log'))

import shutil
def wipe_test_db():
    db_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.snerdata'))
    if os.path.exists(db_dir):
        shutil.rmtree(db_dir)

@pytest.mark.asyncio
async def test_snerdqueue_integration():
    wipe_test_db()
    
    queue = SnerdQueue(binary_path=BIN_PATH)
    
    # We will use an asyncio Event to block the test until the handler runs
    job_completed = asyncio.Event()

    async def my_handler(data):
        print(f"Handler called with data: {data}")
        assert data['user_id'] == 'john_wick'
        assert data['message'] == 'Baba Yaga'
        job_completed.set()
        print("Handler finished")

    queue.register_handler('test_notification', my_handler)
    
    # Start the daemon listener as a background task
    listener_task = asyncio.create_task(queue.start_listening())
    print("Started listening")
    
    # Give the daemon a tiny fraction of a second to boot up before enqueuing
    await asyncio.sleep(0.5) # Increased wait to let daemon start
    
    # Enqueue a test job
    print("Enqueuing...")
    await queue.enqueue(
        task_id='pytest-job-1',
        task_type='test_notification',
        data={'user_id': 'john_wick', 'message': 'Baba Yaga'}
    )
    print("Enqueued successfully")

    # Wait for the handler to fire (timeout after 10 seconds if it fails)
    try:
        print("Waiting for handler to complete...")
        await asyncio.wait_for(job_completed.wait(), timeout=10.0)
        print("Test passed!")
    finally:
        # Gracefully shut down the rust daemon
        await queue.shutdown()
        # Cancel the listener so pytest can exit cleanly
        listener_task.cancel()
        try:
            await listener_task
        except asyncio.CancelledError:
            pass
