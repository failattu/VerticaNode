var restify = require("restify");
var restifyOAuth2 = require("restify-oauth2");
var cluster = require('cluster');
var vertica = require("Vertica");
var workers = require('os').cpus().length;

var config = {host: '192.168.192.128', user: "dbadmin", password: 'password', database:"VMart"};
var server = restify.createServer({
    name: "Very Simple Application Analytics Server",
    version: "0.0.3",
    formatters: {
        "application/hal+json": function (req, res, body) {
            return res.formatters["application/json"](req, res, body);
        }
    }
});

var connection = new vertica.connect(config, function (err) {
    // connected! (unless `err` is set)
    if (err) {
        console.log("Error while connecting to database.");
    } else {
        console.log("Database connection successful.");
    }
});

server.get("/", function (req, res) {

    query = connection.query("Select A.annual_income As AVGinc, Count(B.customer_key) As AVGOrders, Avg(B.cost_dollar_amount) As AVGCost From public.customer_dimension As A Inner Join  online_sales.online_sales_fact As B On A.customer_key = B.customer_key group by A.annual_income ORDER BY AVGinc desc", function(err, result) {
        if (err) console.log(err);
        //TODO: Convert to JSON
        console.log(result.fields);
        var x = result.rows;
        console.log(x);
    });
   // console.log(query);
   // res.contentType = "application/hal+json";
    //res.send(response);
});

// Clustering to utilize all CPU cores
if (cluster.isMaster) {

    // Fork workers
    for (var i = 0; i < workers; i++) {
        cluster.fork();
    }

    cluster.on('exit', function (worker, code, signal) {
        console.log('Worker ' + worker.process.pid + ' died');
        console.log('Spawining new worker...');
        cluster.fork();
    });
}
else {
    server.listen(8080);
}
