const replay = require("replay");
const request = require("request");
const async = require("async");
const elementTypes = ["agent", "space", "project", "event"];

const installs = [
  {
    name: "ceara",
    url: "http://mapa.cultura.ce.gov.br/api/"
  },
  {
    name: "spcultura",
    url: "http://spcultura.prefeitura.sp.gov.br/api/"
  }
];
const mongodbConnectionString = "mongodb://localhost:27018/mapasculturais";
const limitPerPage = 300;

/*
 * Init database
 */

var mongo = require("mongodb").MongoClient;
var dbConnection;
mongo.connect(mongodbConnectionString, function(err, db) {
  if (err) {
    console.log(err.message);
    return;
  }

  console.log("Connected to MongoDB.");

  dbConnection = db;
  runImport(function(err) {
    if (err) console.log(err.message);
    db.close();
  });
});

/*
 * Init import
 */
function runImport(doneRunImport) {
  async.eachSeries(installs, fetchInstallData, doneRunImport);
}

/*
 * Fetch data for an install
 */

function fetchInstallData(install, doneFetchInstallData) {
  console.log(`Fetching data from ${install.name}...`);
  async.eachSeries(
    elementTypes,
    function(type, doneType) {
      fetchTypeData(install, type, doneType);
    },
    doneFetchInstallData
  );
}

/*
 * Fetch data for a specific type
 */

function fetchTypeData(install, type, doneFetchElementType) {
  var page = 0;
  var data = [];

  async.doUntil(
    function(next) {
      page++;
      request(
        {
          uri: install.url + type + "/find",
          qs: {
            "@SELECT": "*",
            "@LIMIT": limitPerPage,
            "@PAGE": page
          }
        },
        function(err, res) {
          if (err) return next(err);
          else {
            // parse result
            var result = JSON.parse(res.body);

            // append to data
            data = data.concat(result);

            // log
            if (result.length > 0)
              console.log(`${data.length} ${type}s fetched...`);

            next(null, result);
          }
        }
      );
    },
    function(result) {
      return result.length == 0;
    },
    function(err) {
      if (err) return doneFetchElementType(err);
      else importData(install, type, data, doneFetchElementType);
    }
  );
}

/*
 * Import data
 */

function importData(install, type, data, doneImportData) {
  console.log(`Importing ${type}s...`);

  var collection = dbConnection.collection(`${install.name}-${type}s`);

  collection.createIndex("_id");

  async.eachSeries(
    data,
    function(item, doneUpdateItem) {
      item._id = item.id;
      collection.updateOne(
        { _id: item.id },
        item,
        { upsert: true },
        doneUpdateItem
      );
    },
    function(err) {
      if (err) console.log(err.message);
      else console.log(`Imported ${data.length} ${type}s.`);
      doneImportData(err);
    }
  );
}
