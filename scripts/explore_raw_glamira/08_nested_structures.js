// Explore nested field structures
// use countly;

// Find documents with 'option' field
db.summary.findOne({
  option: { $exists: true, $ne: null, $ne: "" }
});

// Find documents with 'cart_products' field
db.summary.findOne({
  cart_products: { $exists: true, $ne: null, $ne: "" }
});

// Get sample of each nested field
db.summary.aggregate([
  { $match: { option: { $exists: true, $ne: null } } },
  { $limit: 10 },
  { $project: { option: 1 } }
]);

db.summary.aggregate([
  { $match: { cart_products: { $exists: true, $ne: null } } },
  { $limit: 10 },
  { $project: { cart_products: 1 } }
]);
