'use strict';

var RestoreDynamoDb = require('./../../lib/bin/restoreDynamoDb');

// Minimal aws-sdk-style client whose batchWriteItem returns the queued
// responses in order. Each response is exposed via a `.promise()` call.
function makeClient(responses) {
    let call = 0;
    const calls = [];
    return {
        calls,
        batchWriteItem(params) {
            calls.push(params);
            const response = responses[Math.min(call, responses.length - 1)];
            call++;
            return { promise: () => Promise.resolve(response) };
        }
    };
}

function newRestore() {
    const restore = new RestoreDynamoDb({
        S3Bucket: 'bucket',
        S3Region: 'eu-west-1',
        DbTable: 'my-table',
        DbRegion: 'eu-west-1'
    });
    // Skip real backoff delays so retry tests run instantly.
    restore.wait = () => Promise.resolve();
    return restore;
}

describe('RestoreDynamoDb.batchWrite', function () {
    it('writes once when there are no unprocessed items', async function () {
        const restore = newRestore();
        const client = makeClient([{ UnprocessedItems: {} }]);
        const dParams = { RequestItems: { 'my-table': [{ PutRequest: { Item: {} } }] } };

        await restore.batchWrite(client, dParams);

        expect(client.calls.length).toBe(1);
        expect(client.calls[0]).toEqual(dParams);
    });

    it('retries only the unprocessed items until the write succeeds', async function () {
        const restore = newRestore();
        const leftover = { 'my-table': [{ PutRequest: { Item: { id: { S: '1' } } } }] };
        const client = makeClient([
            { UnprocessedItems: leftover }, // first attempt: throttled
            { UnprocessedItems: {} }        // retry: succeeds
        ]);
        const dParams = { RequestItems: { 'my-table': [{ PutRequest: { Item: { id: { S: '1' } } } }] } };

        await restore.batchWrite(client, dParams);

        expect(client.calls.length).toBe(2);
        // The retry must re-send ONLY the unprocessed items, wrapped in RequestItems.
        expect(client.calls[1]).toEqual({ RequestItems: leftover });
    });

    it('throws once retries are exhausted so data loss is never silent', async function () {
        const restore = newRestore();
        const leftover = { 'my-table': [{ PutRequest: { Item: {} } }] };
        // Always returns unprocessed items -> retry ceiling (10) is hit.
        const client = makeClient([{ UnprocessedItems: leftover }]);
        const dParams = { RequestItems: leftover };

        let thrown;
        try {
            await restore.batchWrite(client, dParams);
        } catch (err) {
            thrown = err;
        }

        expect(thrown).toBeDefined();
        expect(thrown.message).toContain('Failed to write all items');
        // Initial attempt + 11 retries (retryCount 0..10, throws when > 10).
        expect(client.calls.length).toBe(12);
    });
});
