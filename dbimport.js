const csv = require("fast-csv");
const util = require('util');
const MongoClient = require('mongodb').MongoClient;

var dataArr = [];
csv.parseFile("greenhouse_gas_inventory_data_data.csv", {headers: true})
.on("data", data => {
  dataArr.push(data);
})
.on("end", async () => {
  console.log(dataArr.length);
  // > 4187
  const uri = "mongodb://localhost:27017";
   const client = new MongoClient(uri,{ useNewUrlParser: true, useUnifiedTopology: true });
 
  try {
      // Connect to the MongoDB cluster
      await client.connect();

      // Make the appropriate DB calls
      await  insertFile(client,dataArr);

  } catch (e) {
      console.error(e);
  } finally {
      await client.close();
  }
});

async function insertFile(client,dataArr) {
  const database = client.db('geoassignment');
  let country_list = await database.collection("country").find({}).toArray();
  console.log("country List",country_list,dataArr[0]);
  let country_overview = {};
  let country_details = {};
  for(let k=0;k<dataArr.length;k++) {
    let element = dataArr[k];
    let category = element.category.split("_emissions")[0].split("_");

    if(country_details[element.country_or_area]) {
      if(country_details[element.country_or_area][element.year]) {
        country_details[element.country_or_area][element.year].push({
          value: element.value,
          category: category,
          category_desc: element.category
        });
      }
      else {
        country_details[element.country_or_area][element.year] = [{
          value: element.value,
          category: category,
          category_desc: element.category
        }];
      }
      if(parseInt(element.year) < country_overview[element.country_or_area]["startYear"]) {
        country_overview[element.country_or_area]["startYear"] = parseInt(element.year);
        country_overview[element.country_or_area]["startDetails"] = [{value: element.value,category: element.category}];
      }
      if(parseInt(element.year) > country_overview[element.country_or_area]["endYear"]) {
        country_overview[element.country_or_area]["endYear"] = parseInt(element.year);
        country_overview[element.country_or_area]["endDetails"] = [{value: element.value,category: element.category}];
      }
      if(parseInt(element.year) == country_overview[element.country_or_area]["startYear"]) {
        country_overview[element.country_or_area]["startDetails"].push({value: element.value,category: element.category});
      }
      if(parseInt(element.year) == country_overview[element.country_or_area]["endYear"]) {
        country_overview[element.country_or_area]["endDetails"].push({value: element.value,category: element.category});
      }
    }
    else {
      country_details[element.country_or_area] = {};
      country_overview[element.country_or_area] = {};
      country_details[element.country_or_area][element.year] = [{
        value: element.value,
        category: category,
        category_desc: element.category
      }];
      country_overview[element.country_or_area]["startYear"] = parseInt(element.year);
      country_overview[element.country_or_area]["startDetails"] = [{value: element.value,category: element.category}];
      country_overview[element.country_or_area]["endYear"] = parseInt(element.year);
      country_overview[element.country_or_area]["endDetails"] = [{value: element.value,category: element.category}];
      }
    }
    let dbOverviewList = [];
    for(let key in country_overview) {
      dbOverviewList.push({
        country_name: key,
        ...country_overview[key]
      });
    }
    let result = await database.collection("country_overview").insertMany(dbOverviewList);
    let detailedList = [];
    for(let p=0;p<result.ops.length;p++) {
      let temp_Coun = result.ops[p];
      for(let m in country_details[temp_Coun.country_name]) {
        detailedList.push({
          country_id: temp_Coun._id,
          country_name: temp_Coun.country_name,
          year: parseInt(m),
          values: country_details[temp_Coun.country_name][m]
        })
      }
    }
    await database.collection("country").insertMany(detailedList);
    
  }
