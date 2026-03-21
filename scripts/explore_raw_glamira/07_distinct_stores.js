// Get distinct store_id values

// use countly;

// Get distinct store_id
db.summary.distinct("store_id");

// Count documents per store
db.summary.aggregate([
  {
    $group: {
      _id: "$store_id",
      count: { $sum: 1 }
    }
  },
  { $sort: { count: -1 } }
]);

//
db.summary.find(
  { store_id: "6" },
  { current_url: 1, _id: 0 }
).limit(5);
