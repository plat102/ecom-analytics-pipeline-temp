// Get timestamp range
// use countly;

// Get exact min/max
db.summary.find().sort({ time_stamp: 1 }).limit(1);  // Minimum
db.summary.find().sort({ time_stamp: -1 }).limit(1); // Maximum
