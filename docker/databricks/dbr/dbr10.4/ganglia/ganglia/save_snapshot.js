// This script takes a snapshot of the ganglia UI and saves it as a timestamped image file
// Example snapshot file : /databricks/driver/ganglia/snapshot-2017-07-14T17:25:02.256Z.png
// These files will be uploaded by log daemon every 10 minutes, and garbage collected every hour
// Reference: http://phantomjs.org/screen-capture.html

const page = require('webpage').create();
page.settings.resourceTimeout = 10000; // 10 seconds timeout
page.onResourceTimeout = function(e) {
  console.log('Error fetching url: ' + e.url + ". Error: " + e.errorCode + ': ' + e.errorString);
  // ES-9631: don't exit in this callback! this will cause the snapshot to fail when
  // a single remote page could not be fetched, such as remote css files.
  // Instead, log and continue in the hope that the snapshotting process completes.
};

const datetime = new Date().toISOString();
const imagePath = '/databricks/driver/ganglia/snapshot-' + datetime + '.png';

const url = 'http://localhost/?c=cluster';
// The height is just a minimum size, actual screenshot scales to fit the entire page
page.viewportSize = { width: 1200, height: 1500 };
page.open(url, function() {
  page.render(imagePath);
  console.log('Ganglia UI snapshot saved to ' + imagePath);
  phantom.exit();
});
