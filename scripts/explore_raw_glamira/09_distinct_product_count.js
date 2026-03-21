// Count distinct products

// Count total distinct product_id values
db.summary.aggregate([
  { $match: { product_id: { $ne: null, $ne: "" } } },
  { $group: { _id: "$product_id" } },
  { $count: "total_distinct_products" }
]);
