const express = require('express');
const app = express();
const MongoClient = require('mongodb').MongoClient;
const uri = "mongodb://localhost:27017";
const client = new MongoClient(uri,{ useNewUrlParser: true, useUnifiedTopology: true });
var database;
var ObjectId = require('mongodb').ObjectID;

const swaggerJsDoc = require("swagger-jsdoc");
const swaggerUi = require("swagger-ui-express");


const PORT = process.env.PORT || 1234;

const swaggerOptions = {
    swaggerDefinition: {
        info: {
            title: "Greenhouse Gas Emission Api",
            description: "Gas Emission Details of Green House",
            contact: {
                name: "Sarthak Kaushik"
            },
            servers: ["http://localhost:1234"]
        }
    },
    apis: ["index.js"]
};

const swaggerDocs = swaggerJsDoc(swaggerOptions);

app.use("/api-docs",swaggerUi.serve,swaggerUi.setup(swaggerDocs));

// Routes
/**
 * @swagger
 * /countries:
 *  get:
 *      desciption: Get all Countries List with start Year details and end Year Details
 *      responses:
 *          '201':
 *              description: Successfull Response
 *              content:
 *                  application/json:
 *                      schema: 
                            type: array
 */
app.get('/countries',async (req,res)=>{
    console.log("countries api");
   let listCountry =  await database.collection('country_overview').find({}).toArray();
    res.status(200).send(listCountry);
});

/**
 * @swagger
 * /country/{id}:
 *  get:
 *      summary: Get country details based on country id and Date filter
 *      parameters:     
 *        - name: id
 *          in: path
 *          required: true
 *          description: The ID of the country
 *        - name: startdate
 *          in: query
 *          required: false
 *          description: Start Date for Filter
 *        - name: enddate
 *          in: query
 *          required: false
 *          description: End Date for Filter
 *        - name: parameters
 *          in: query
 *          required: false
 *          description: Pipe(|) Seprerated Parameter Value (ex; co2|n2o or co2)
 *      responses:
 *          '201':
 *              description: Successfull Response
 *              content:
 *                  application/json:
 *                      schema: 
                            type: array
 */

app.get('/country/:id',async (req,res)=>{
    let dbQuery = {
        $and: [{country_id: ObjectId(req.params.id)}]
    };
    if(req.query) {
        if(req.query.startdate && !req.query.startdate.match(/^[1-9]\d{3,}$/)) {
            res.status(401).send({status: false, error: "Invalid Start Date"});
            return;
        }
        if(req.query.enddate && !req.query.enddate.match(/^[1-9]\d{3,}$/)) {
            res.status(401).send({status: false, error: "Invalid End Date"});
            return;
        }
        if(req.query.startdate && req.query.enddate) {
            dbQuery["$and"].push({ year: { $gte: parseInt(req.query.startdate), $lte: parseInt(req.query.enddate)}});
        }
        else if(req.query.startdate) {
            dbQuery["$and"].push({ year: { $gte: parseInt(req.query.startdate)}});
        }
        else if(req.query.enddate) {
            dbQuery["$and"].push({ year: { $lte: parseInt(req.query.enddate)}});
        }

        if(req.query.parameters) {
            let listParams = req.query.parameters.split("|");
            dbQuery["$and"].push({"values.category": {$in: listParams}});
        }
    }
    let country_details = await database.collection('country').find(dbQuery).toArray();
    if(country_details.length == 0) {
        res.status(401).send({status: false, error: "Data Does not exist"});
        return;
    }
    if(req.query.parameters) {
        let gasList = req.query.parameters.split("|");
        for(let k=0;k<country_details.length;k++) {
            let countryValue = [];
            for(p=0;p<country_details[k]["values"].length;p++) {
                let intersection = country_details[k]["values"][p].category.filter(value => gasList.includes(value));
                if(intersection && intersection.length > 0) {
                    countryValue.push(country_details[k]["values"][p]);
                }
            }
            country_details[k]["values"] = countryValue;
        }
    }
    res.status(200).send(country_details);
});

app.get("/",(req,res)=>{
    res.send("Invalid Api");
})

app.listen(PORT,async ()=>{
    await client.connect();
    database = client.db('geoassignment');
    console.log("Listening to port ",PORT);
});