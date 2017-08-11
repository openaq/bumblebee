var aws = require('aws-sdk');
var flatten = require('lodash.flatten');
var pLimit = require('p-limit');
var pify = require('pify');

var s3 = new aws.S3();
var listObjects = pify(s3.listObjectsV2.bind(s3));
var getObject = pify(s3.getObject.bind(s3));
var putObject = pify(s3.putObject.bind(s3));

async function handler(e, ctx, callback) {
  var event = e.Records[0];

  /** 
   * Key will be of the form [bucket]/realtime/{yyyy-mm-dd}/{unixtime}.ndjson
   * we want to 
   * 1. extract the prefix [bucket]/realtime/{yyyy-mm-dd}/
   * 2. read all the files for that day
   * 3. convert all the files to CSV
   * 4. concatenate them and post to [bucket]/daily/{yyyy-mm-dd}.csv
   */
  var key = event.s3.object.key;
  var parts = key.split('/');
  var bucket = event.s3.bucket.name;
  var date = parts[1];
  var filename = parts.pop(); 
  var prefix = parts.join('/');
  console.info('Got event for key:', key);

  console.info(`Reading ${bucket}/${prefix}`);
  try {
    let rows = await concatenateDir(bucket, prefix);
    console.info(`Files successfully concatenated`);
  } catch (err) {
    console.error('Error concatenating files'); 
    return callback(err);
  }

  // file is a string of ndjson
  console.info(`There are ${rows.length} records`);
  var csvRows = rows.map((data, idx) => {
    let d = {}
    try {
      d = JSON.parse(data);
      return buildCSVRow(d);
    } catch (e) {
      console.error(e, idx, data);
      callback(e);
    }
  });

  try {
    console.info(`Adding CSV with ${csvRows.length} records to S3`);

    // Join into line delimited file
    var csv = csvRows.join('\n');

    // Put in S3
    let success = await putObject({ Bucket: bucket, Key: `daily/${date}.csv`, Body: csv});
    callback(null, success);
  } catch (err) {
    console.error('Error adding concatenated file to S3');
    return callback(err);
  }
}

/**
 * Concatenates file contents from an S3 directory and returns an array
 * of measurements
 * @param {string} bucket - S3 bucket
 * @param {string} key - Key representing a directory from which to read files
 */
async function concatenateDir (bucket, key) {
  const limit = pLimit(1);
  let list = await listObjects({ Bucket: bucket, Prefix: key});
  let tasks = list.Contents.map(item => {
    return limit(() => getObject({Bucket: bucket, Key: item.Key}));
  });

  return Promise.all(tasks).then(results => {
    let contents = results
      .map(result => {
        return result.Body.toString()
          .split('\n')
          .filter(str => str.length > 0);
      });
    return flatten(contents);
  });
}

/**
 * Build a CSV row from a measurement
 * Header: location, value, unit, parameter, country, city, sourceName, date_utc, date_local, sourceType, mobile, latitude, longitude
 * @param {object} m measurement object
 * @return {string} a string representing one row in a CSV
 */
function buildCSVRow (m) {
  var row = [
    m.location,
    m.value,
    m.unit,
    m.parameter,
    m.country,
    m.city,
    m.sourceName,
    new Date(m.date.utc).toISOString(),
    m.date.local,
    m.sourceType,
    m.mobile
  ];
  var latitude = '';
  var longitude = '';

  // Handle geo
  if (m.coordinates) {
    latitude = m.coordinates.latitude || '';
    longitude = m.coordinates.longitude || '';
  }
  row.push(latitude);
  row.push(longitude);

  // Add double quotes to each field
  var quotedRow = row.map(item => `"${item}"`);

  return quotedRow.join(',');
};

module.exports = {
  handler
}
