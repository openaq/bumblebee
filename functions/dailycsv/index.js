var aws = require('aws-sdk');
var flatten = require('lodash.flatten');
var async = require('async');
var s3 = new aws.S3();

module.exports.handler = function (e, ctx, callback) {
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
  concatenateDir(bucket, prefix, (err, rows) => {
    if (err) {
      console.error('Error concatenating files');
      return callback(err);
    }

    console.info(`Files successfully concatenated`);
    // file is a string of ndjson
    var jsonRows = rows.filter(row => row.length > 0)
    console.info(`There are ${jsonRows.length} records`);
    var csvRows = jsonRows.map((data, idx) => {
      let d = {}
      try {
        d = JSON.parse(data);
        return buildCSVRow(d);
      } catch (e) {
        console.error(e, idx, data);
        callback(e);
      }
    });

    // Join into line delimited file
    var csv = csvRows.join('\n');

    console.info(`Adding CSV with ${csvRows.length} records to S3`);
    s3.putObject({
      Bucket: bucket, 
      Key: `daily/${date}.csv`,
      Body: csv
    }, function (err, data) {
      if (err) {
        console.error('Error adding concatenated file to S3');
        return callback(err);
      }
      else return callback(null, data);
    })
  });
}

/**
 * Concatenates file contents from an S3 directory and returns an array
 * of measurements
 * @param {string} bucket - S3 bucket
 * @param {string} key - Key representing a directory from which to read files
 * @param concatenatDirCallback callback - Callback on function error or success
 */
function concatenateDir (bucket, key, callback) {
  s3.listObjectsV2({
    Bucket: bucket, 
    Prefix: key
  }, function (err, data) {
    if (err) return callback(err);
    else {

      // Download 
      var tasks = data.Contents.map(function (Obj) {
        return function (done) {
          s3.getObject({Bucket: bucket, Key: Obj.Key}, function (err, data) {
            if (err) done(err);
            else done(null, data);
          });
        }
      });

      async.parallel(tasks, function (err, results) {
        if (err) callback(err);
        else {
          let contents = results
            .map(result => result.Body.toString().split('\n'));
          callback(null, flatten(contents));
        }
      });
    }
  })
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
