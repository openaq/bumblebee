var aws = require('aws-sdk');
var pify = require('pify');
var pull = require('pull-stream');
var paramap = require('pull-paramap');
var file = require('pull-file');
var Write = require('pull-write-file');
var split = require('pull-split');
var utf8 = require('pull-utf8-decoder');

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

  let totaljson = '/tmp/out.ndjson';

  console.info(`Reading ${bucket}/${prefix}`);
  try {
    await pify(concatenateDir)(bucket, prefix, totaljson);
    console.info(`Files successfully concatenated.`);
  } catch (err) {
    console.error('Error concatenating files.'); 
    return callback(err);
  }

  // Read back the concatenated file, transform it and push to S3
  pull(
    file(totaljson),
    utf8(),
    split('\n', null, false, true),
    pull.map(x => {
      try {
        return JSON.parse(x);
      } catch (e) {
        console.error(e, x); 
        callback(e);
      }
    }),
    pull.map(buildCSVRow),
    pull.collect(async function (err, values) {
      console.info(`Pushing ${values.length} records to an S3 CSV.`)
      if (err) callback(err);
      let success = await putObject({ 
        Bucket: bucket, 
        Key: `daily/${date}.csv`, 
        Body: values.join('\n')
      });
      callback(null, success);
    })
  )
}

/**
 * Concatenates file contents from an S3 directory into an outfile
 * @param {string} bucket - S3 bucket
 * @param {string} key - Key representing a directory from which to read files
 * @param {string} outfile - Where to write the contents of the S3 bucket
 */
async function concatenateDir (bucket, key, outfile, callback) {
  let list = await listObjects({ Bucket: bucket, Prefix: key});

  pull(
    pull.values(list.Contents),
    paramap((item, cb) => {
      s3.getObject({Bucket: bucket, Key: item.Key}, cb)
    }, 1),
    pull.map(result => Buffer.concat([result.Body, new Buffer('\n')])),
    Write(outfile, callback)
  );
}

/**
 * Build a CSV row from a measurement
 * Header: location, value, unit, parameter, country, city, sourceName, date_utc, date_local, 
 *  sourceType, mobile, latitude, longitude, averagingPeriodValue, averagingPeriodUnit
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

  // Handle geo
  var latitude = '';
  var longitude = '';
  if (m.coordinates) {
    latitude = m.coordinates.latitude || '';
    longitude = m.coordinates.longitude || '';
  }
  row.push(latitude);
  row.push(longitude);

  // Handle averaging
  var period = '';
  var unit = '';
  if (m.averagingPeriod) {
    period = m.averagingPeriod.value,
    unit = m.averagingPeriod.unit
  }
  row.push(period);
  row.push(unit);

  // Add double quotes to each field
  var quotedRow = row.map(item => `"${item}"`);

  return quotedRow.join(',');
};

module.exports = {
  handler
}
