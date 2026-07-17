"use strict";

const AWS = require("aws-sdk");

class RestoreDynamoDb {
  constructor(config) {
    this.S3Bucket = config.S3Bucket;
    this.S3Region = config.S3Region;
    this.DbTable = config.DbTable;
    this.DbRegion = config.DbRegion;
    this.wait = (ms) => new Promise((res) => setTimeout(res, ms));
  }

  async s3ToDynamoDb(versionList, merged) {
    return this.getDbTableKeys()
      .then((keys) => {
        let promises = [];
        Object.keys(versionList).forEach((key, index) => {
          promises.push(this.processVersion(versionList[key], keys, merged));
        });

        return Promise.all(promises);
      })
      .catch((err) => {
        throw err;
      });
  }

  async getDbTableKeys() {
    console.time("getDbTableKeys");
    return new Promise((resolve, reject) => {
      let dynamoDb = new AWS.DynamoDB({ region: this.DbRegion });
      dynamoDb.describeTable({ TableName: this.DbTable }, (err, data) => {
        if (err) {
          return reject(err);
        }
        return resolve(data.Table.KeySchema);
      });
    }, console.timeEnd("getDbTableKeys"));
  }

  async processVersion(version, keys, merged) {
    console.time("processVersion");
    return this.retrieveFromS3(version)
      .then((data) => {
        // if (merged == true) {
        return this.pushMergedToDynamoDb(data[1]);
        // } else {
        //   return await this.pushToDynamoDb(data[0], data[1], keys);
        // }
      }, console.timeEnd("processVersion"))
      .catch((err) => {
        throw err;
      });
  }

  async retrieveFromS3(version) {
    console.time("Retrieve from S3 " + version.Key);
    let params = {
      Bucket: this.S3Bucket,
      Key: version.Key,
      VersionId: version.VersionId,
    };
    let s3 = new AWS.S3({ region: this.S3Region, signatureVersion: "v4" });
    return new Promise((resolve, reject) => {
      s3.getObject(params, (err, data) => {
        if (err) {
          return reject(
            "Failed to retrieve file from S3 - Params: " +
              JSON.stringify(params)
          );
        }

        return resolve([version, JSON.parse(data.Body.toString("utf-8"))]);
      });
    }, console.timeEnd("Retrieve from S3 " + version.Key));
  }

  async pushMergedToDynamoDb(records) {
    console.time("pushMergedToDynamoDb");
    let dParams = { RequestItems: {} };
    dParams.RequestItems[this.DbTable] = [];

    let batches = [];
    let current_batch = [];
    let item_count = 0;

    records.forEach((record) => {
      item_count++;
      current_batch.push({
        PutRequest: {
          Item: JSON.parse(record),
        },
      });

      if (item_count % 25 === 0) {
        batches.push(current_batch);
        current_batch = [];
      }
    });

    if (current_batch.length > 0 && current_batch.length !== 25) {
      batches.push(current_batch);
    }

    let dynamoDbClient = new AWS.DynamoDB({ region: this.DbRegion });
    let promises = [];

    console.log(`Number of batches to restore ${batches.length}`);
    for (let i = 0; i < batches.length; i++) {
      //console.log(`Restoring batch index ${i}`);
      dParams.RequestItems[this.DbTable] = batches[i];

      promises.push(await this.batchWrite(dynamoDbClient, dParams));
    }
    console.timeEnd("pushMergedToDynamoDb");
    return Promise.all(promises);
  }

  async batchWrite(dynamoDb, dParams, retryCount = 0) {
    const res = await dynamoDb.batchWriteItem(dParams).promise();

    // UnprocessedItems is a map keyed by table name ({ [TableName]: [WriteRequest] }),
    // not an array, so `.length` is always undefined. Detect leftover work by checking
    // whether the map holds any entries.
    const unprocessed = res.UnprocessedItems;
    const hasUnprocessed = unprocessed && Object.keys(unprocessed).length > 0;

    if (hasUnprocessed) {
      if (retryCount > 10) {
        throw new Error(
          `Failed to write all items after ${retryCount} retries. Unprocessed items remain: ${JSON.stringify(
            unprocessed
          )}`
        );
      }
      await this.wait(2 ** retryCount * 50);

      // Retry only the items DynamoDB could not process, wrapped back into the
      // RequestItems shape batchWriteItem expects, via `this.batchWrite`.
      return this.batchWrite(
        dynamoDb,
        { RequestItems: unprocessed },
        retryCount + 1
      );
    }
  }

  async pushToDynamoDb(version, fileContents, keys) {
    return new Promise(async (resolve, reject) => {
      let action = {};
      let dParams = { RequestItems: {} };
      dParams.RequestItems[this.DbTable] = [];

      if (!version.DeletedMarker) {
        Object.keys(fileContents).forEach((attributeName) => {
          // Fix JSON.stringified Binary data
          let attr = fileContents[attributeName];
          if (
            attr.B &&
            attr.B.type &&
            attr.B.type === "Buffer" &&
            attr.B.data
          ) {
            attr.B = Buffer.from(attr.B.data);
          }
        });
        action.PutRequest = {
          Item: fileContents,
        };
      } else {
        action.DeleteRequest = {
          Key: {},
        };
        keys.forEach((key) => {
          action.DeleteRequest.Key[key.AttributeName] =
            fileContents[key.AttributeName];
        });
      }
      dParams.RequestItems[this.DbTable].push(action);

      let dynamoDb = new AWS.DynamoDB({ region: this.DbRegion });
      //console.time("P2D " + version.Key);
      await dynamoDb.batchWriteItem(dParams, (err, data) => {
        //console.timeEnd("P2D " + version.Key);
        if (err) {
          return reject(err);
        }
        return resolve(data);
      });
    });
  }
}

module.exports = RestoreDynamoDb;
