/**
 * Management Console Jobs
 * @module jobs
 */

var request = require("request");
var CronJob = require("cron").CronJob;
var path = require("path");
var fs = require("fs");
var JSZip = require("jszip");
var async = require("async");

var analytics = require("./lib/analytics");
var analyticsSpotlight = require("./lib/analyticsSpotlight");
var analyticsModified = require("./lib/analyticsModified");

var env = {
	mode: "DEV",
	debug: false
};
if (process.env.FR_ROLE) {
	env.mode = (process.env.FR_ROLE).toUpperCase();
}
if (process.env.FR_DEBUG_MODE) {
	env.debug = (process.env.FR_DEBUG_MODE == "true");
}

var port = null;
var dbConfig = null;
var jobsConfig = null;
var jobs = {};

/**
 * Initialize jobs
 */
function init() {
	/** Initialize port for local API calls */
	port = normalizePort(process.env.PORT || "5990");
	
	/** Initialize jobs configuration */
	var cloudantConfig;
	var dbCredentialsConfig = {};
	if (process.env.CLOUDANT_R) {
		var cloudantR = JSON.parse(process.env.CLOUDANT_R);
		if(cloudantR) {
			dbCredentialsConfig.host = cloudantR.cloudant.host;
			dbCredentialsConfig.user = cloudantR.cloudant.username;
			dbCredentialsConfig.password = cloudantR.cloudant.password;
			dbCredentialsConfig.dbName = cloudantR.cloudant.db;
			dbCredentialsConfig.url = "https://" + dbCredentialsConfig.user + ":" + dbCredentialsConfig.password + "@" + dbCredentialsConfig.host;
		}
	} else {
		console.error((new Date()).toISOString(), "[jobs.init]", "ERROR: CLOUDANT_R environment var is not set. It needs to have credentials to the configuration DB.");
		console.error("export CLOUDANT_R=\"{\"cloudant\":{\"db\":\"databasename\",\"username\":\"username\",\"password\":\"password\",\"host\":\"hostname\"}}\"");
	}
	cloudantConfig = require('cloudant')(dbCredentialsConfig.url);
	dbConfig = cloudantConfig.use(dbCredentialsConfig.dbName);
	
	dbConfig.get("module_09_managementux", function(err, result) {
		if (err) {
			console.error((new Date()).toISOString(), "[jobs.init]", "ERROR: failed to get managementux configuration doc, jobs will NOT be started");
			console.error(err);
		} else {
			if (env.mode === "PROD") {
				jobsConfig = result.jobs;
			} else {
				jobsConfig = result.jobs_test;
			}
			console.log((new Date()).toISOString(), "[jobs.init]", "jobsConfig initialized");
			console.log(JSON.stringify(jobsConfig, null, "\t"));
			
			/** Initialize jobs */
			jobs["refresh_config"] = new CronJob(jobsConfig.refresh_config.cron, refreshConfig, null, false);
			jobs["save_time_series_data"] = new CronJob(jobsConfig.save_time_series_data.cron, function() {saveTimeSeriesData(jobsConfig.save_time_series_data)}, null, false);
			jobs["refresh_solution_stats"] = new CronJob(jobsConfig.refresh_solution_stats.cron, function() {refreshSolutionStats(jobsConfig.refresh_solution_stats)}, null, false);
			jobs["refresh_spotlight_solution_stats"] = new CronJob(jobsConfig.refresh_spotlight_solution_stats.cron, function() {refreshSpotlightSolutionStats(jobsConfig.refresh_spotlight_solution_stats)}, null, false);
			jobs["refresh_modified_solution_stats"] = new CronJob(jobsConfig.refresh_modified_solution_stats.cron, function() {refreshModifiedSolutionStats(jobsConfig.refresh_modified_solution_stats)}, null, false);
			jobs["refresh_tickets_updated_kmm_data"] = new CronJob(jobsConfig.refresh_tickets_updated_kmm_data.cron, function() {refreshTicketsUpdatedKMMData(jobsConfig.refresh_tickets_updated_kmm_data)}, null, false);
			jobs["refresh_spotlight_tickets_updated_kmm_data"] = new CronJob(jobsConfig.refresh_spotlight_tickets_updated_kmm_data.cron, function() {refreshSpotlightTicketsUpdatedKMMData(jobsConfig.refresh_spotlight_tickets_updated_kmm_data)}, null, false);
			jobs["refresh_tickets_updated_kmm_data_hourly"] = new CronJob(jobsConfig.refresh_tickets_updated_kmm_data_hourly.cron, function() {refreshTicketsUpdatedKMMData(jobsConfig.refresh_tickets_updated_kmm_data_hourly)}, null, false);
			jobs["refresh_spotlight_tickets_updated_kmm_data_hourly"] = new CronJob(jobsConfig.refresh_spotlight_tickets_updated_kmm_data_hourly.cron, function() {refreshSpotlightTicketsUpdatedKMMData(jobsConfig.refresh_spotlight_tickets_updated_kmm_data_hourly)}, null, false);
			jobs["generate_report_tickets_updated_kmm_sw"] = new CronJob(jobsConfig.generate_report_tickets_updated_kmm_sw.cron, function() {generateReportTicketsUpdatedKMM("sw", jobsConfig.generate_report_tickets_updated_kmm_sw)}, null, false);
			jobs["generate_report_time_series_sw"] = new CronJob(jobsConfig.generate_report_time_series_sw.cron, function() {generateReportTimeSeries("sw", jobsConfig.generate_report_time_series_sw)}, null, false);
			jobs["generate_report_tickets_updated_kmm_hw"] = new CronJob(jobsConfig.generate_report_tickets_updated_kmm_hw.cron, function() {generateReportTicketsUpdatedKMM("hw", jobsConfig.generate_report_tickets_updated_kmm_hw)}, null, false);
			jobs["generate_report_time_series_hw"] = new CronJob(jobsConfig.generate_report_time_series_hw.cron, function() {generateReportTimeSeries("hw", jobsConfig.generate_report_time_series_hw)}, null, false);
			jobs["generate_report_spotlight_tickets_updated_kmm_sw"] = new CronJob(jobsConfig.generate_report_spotlight_tickets_updated_kmm_sw.cron, function() {generateReportSpotlightTicketsUpdatedKMM("sw", jobsConfig.generate_report_spotlight_tickets_updated_kmm_sw)}, null, false);
			jobs["generate_report_spotlight_time_series_sw"] = new CronJob(jobsConfig.generate_report_spotlight_time_series_sw.cron, function() {generateReportSpotlightTimeSeries("sw", jobsConfig.generate_report_spotlight_time_series_sw)}, null, false);
			jobs["generate_report_spotlight_tickets_updated_kmm_hw"] = new CronJob(jobsConfig.generate_report_spotlight_tickets_updated_kmm_hw.cron, function() {generateReportSpotlightTicketsUpdatedKMM("hw", jobsConfig.generate_report_spotlight_tickets_updated_kmm_hw)}, null, false);
			jobs["generate_report_spotlight_time_series_hw"] = new CronJob(jobsConfig.generate_report_spotlight_time_series_hw.cron, function() {generateReportSpotlightTimeSeries("hw", jobsConfig.generate_report_spotlight_time_series_hw)}, null, false);
			jobs["generate_report_modified_time_series_sw"] = new CronJob(jobsConfig.generate_report_modified_time_series_sw.cron, function() {generateReportModifiedTimeSeries("sw", jobsConfig.generate_report_modified_time_series_sw)}, null, false);
			jobs["generate_report_modified_tickets_updated_kmm_sw"] = new CronJob(jobsConfig.generate_report_modified_tickets_updated_kmm_sw.cron, function() {generateReportModifiedTicketsUpdatedKMM("sw", jobsConfig.generate_report_modified_tickets_updated_kmm_sw)}, null, false);

			/** Start jobs automatically only if FR_ROLE=PROD, do NOT automatically start in DEV or TEST */
			if (!env.debug && env.mode === "PROD") {
				startJobs(function(err, result) {
					if (err) {
						console.error((new Date()).toISOString(), "[jobs.init]", "ERROR: failed to start jobs");
						console.error(err);
					}
				});
			}
			
		}
	});
}
init();

/**
 * Refresh Config Job
 * Check if there are changes to the job configuration and refresh
 */
var refreshConfig = function() {
	dbConfig.get("module_09_managementux", function(err, result) {
		if (err) {
			console.error((new Date()).toISOString(), "[refreshConfig]", "ERROR: failed to get managementux configuration doc, cannot refresh configuration");
			console.error(err);
			return;
		}
		
		var newJobsConfig
		if (env.mode === "PROD") {
			newJobsConfig = result.jobs;
		} else {
			newJobsConfig = result.jobs_test;
		}
		
		if (JSON.stringify(jobsConfig) != JSON.stringify(newJobsConfig)) {
			jobsConfig = newJobsConfig;
			console.log((new Date()).toISOString(), "[refreshConfig]", "jobsConfig changed");
			console.log(JSON.stringify(jobsConfig, null, "\t"));
			/** Configuration changed, start jobs with updated configuration */
			stopJobs(function(err, result) {
				if (err) {
					console.error((new Date()).toISOString(), "[jobs.refreshConfig]", "ERROR: failed to stop jobs");
					console.error(err);
				} else {
					/** Initialize jobs with new config */
					jobs["refresh_config"] = new CronJob(jobsConfig.refresh_config.cron, refreshConfig, null, false);
					jobs["save_time_series_data"] = new CronJob(jobsConfig.save_time_series_data.cron, function() {saveTimeSeriesData(jobsConfig.save_time_series_data)}, null, false);
					jobs["refresh_solution_stats"] = new CronJob(jobsConfig.refresh_solution_stats.cron, function() {refreshSolutionStats(jobsConfig.refresh_solution_stats)}, null, false);
					jobs["refresh_spotlight_solution_stats"] = new CronJob(jobsConfig.refresh_spotlight_solution_stats.cron, function() {refreshSpotlightSolutionStats(jobsConfig.refresh_spotlight_solution_stats)}, null, false);
					jobs["refresh_modified_solution_stats"] = new CronJob(jobsConfig.refresh_modified_solution_stats.cron, function() {refreshModifiedSolutionStats(jobsConfig.refresh_modified_solution_stats)}, null, false);
					jobs["refresh_tickets_updated_kmm_data"] = new CronJob(jobsConfig.refresh_tickets_updated_kmm_data.cron, function() {refreshTicketsUpdatedKMMData(jobsConfig.refresh_tickets_updated_kmm_data)}, null, false);
					jobs["refresh_spotlight_tickets_updated_kmm_data"] = new CronJob(jobsConfig.refresh_spotlight_tickets_updated_kmm_data.cron, function() {refreshSpotlightTicketsUpdatedKMMData(jobsConfig.refresh_spotlight_tickets_updated_kmm_data)}, null, false);
					jobs["refresh_tickets_updated_kmm_data_hourly"] = new CronJob(jobsConfig.refresh_tickets_updated_kmm_data_hourly.cron, function() {refreshTicketsUpdatedKMMData(jobsConfig.refresh_tickets_updated_kmm_data_hourly)}, null, false);
					jobs["refresh_spotlight_tickets_updated_kmm_data_hourly"] = new CronJob(jobsConfig.refresh_spotlight_tickets_updated_kmm_data_hourly.cron, function() {refreshSpotlightTicketsUpdatedKMMData(jobsConfig.refresh_spotlight_tickets_updated_kmm_data_hourly)}, null, false);
					jobs["generate_report_tickets_updated_kmm_sw"] = new CronJob(jobsConfig.generate_report_tickets_updated_kmm_sw.cron, function() {generateReportTicketsUpdatedKMM("sw", jobsConfig.generate_report_tickets_updated_kmm_sw)}, null, false);
					jobs["generate_report_time_series_sw"] = new CronJob(jobsConfig.generate_report_time_series_sw.cron, function() {generateReportTimeSeries("sw", jobsConfig.generate_report_time_series_sw)}, null, false);
					jobs["generate_report_tickets_updated_kmm_hw"] = new CronJob(jobsConfig.generate_report_tickets_updated_kmm_hw.cron, function() {generateReportTicketsUpdatedKMM("hw", jobsConfig.generate_report_tickets_updated_kmm_hw)}, null, false);
					jobs["generate_report_time_series_hw"] = new CronJob(jobsConfig.generate_report_time_series_hw.cron, function() {generateReportTimeSeries("hw", jobsConfig.generate_report_time_series_hw)}, null, false);
					jobs["generate_report_spotlight_tickets_updated_kmm_sw"] = new CronJob(jobsConfig.generate_report_spotlight_tickets_updated_kmm_sw.cron, function() {generateReportSpotlightTicketsUpdatedKMM("sw", jobsConfig.generate_report_spotlight_tickets_updated_kmm_sw)}, null, false);
					jobs["generate_report_spotlight_time_series_sw"] = new CronJob(jobsConfig.generate_report_spotlight_time_series_sw.cron, function() {generateReportSpotlightTimeSeries("sw", jobsConfig.generate_report_spotlight_time_series_sw)}, null, false);
					jobs["generate_report_spotlight_tickets_updated_kmm_hw"] = new CronJob(jobsConfig.generate_report_spotlight_tickets_updated_kmm_hw.cron, function() {generateReportSpotlightTicketsUpdatedKMM("hw", jobsConfig.generate_report_spotlight_tickets_updated_kmm_hw)}, null, false);
					jobs["generate_report_spotlight_time_series_hw"] = new CronJob(jobsConfig.generate_report_spotlight_time_series_hw.cron, function() {generateReportSpotlightTimeSeries("hw", jobsConfig.generate_report_spotlight_time_series_hw)}, null, false);
					jobs["generate_report_modified_time_series_sw"] = new CronJob(jobsConfig.generate_report_modified_time_series_sw.cron, function() {generateReportModifiedTimeSeries("sw", jobsConfig.generate_report_modified_time_series_sw)}, null, false);
					jobs["generate_report_modified_tickets_updated_kmm_sw"] = new CronJob(jobsConfig.generate_report_modified_tickets_updated_kmm_sw.cron, function() {generateReportModifiedTicketsUpdatedKMM("sw", jobsConfig.generate_report_modified_tickets_updated_kmm_sw)}, null, false);

					startJobs(function(err, result) {
						if (err) {
							console.error((new Date()).toISOString(), "[jobs.refreshConfig]", "ERROR: failed to start jobs");
							console.error(err);
						}
					});
				}
			});

		} else {
			console.log((new Date()).toISOString(), "[jobs.refreshConfig]", "jobsConfig did NOT change");
		}
	});
};

/**
 * Save Time Series data
 * Periodically save the time series data
 * @param {Object} config - JSON config properties
 */
function saveTimeSeriesData(config) {
	console.log((new Date()).toISOString(), "[jobs.saveTimeSeriesData]", "executing job");
	async.series([
		function(callback) {
			analytics.saveTimeSeries(config.start_date, null, true, config.cache, function(err, result) {
				if (err) {
					callback(err);
				} else {
					callback(null, result);
				}
			});
		},
		function(callback) {
			analyticsSpotlight.saveSpotlightTimeSeries(config.start_date_spotlight, null, true, config.cache, function(err, result) {
				if (err) {
					callback(err);
				} else {
					callback(null, result);
				}
			});
		}
	], function(error, results) {
		if (error) {
			console.error((new Date()).toISOString(), "[jobs.saveTimeSeriesData]", "ERROR: failed to save time series data");
			console.error(err);
		} else {
			console.log(results);
			console.log((new Date()).toISOString(), "[jobs.saveTimeSeriesData]", "saved time series data");
		}
	});
}

/**
 * Refresh Solution Stats Job
 * Periodically refresh the solution stats for the dashboard
 * @param {Object} config - JSON config properties
 */
function refreshSolutionStats(config) {
	console.log((new Date()).toISOString(), "[jobs.refreshSolutionStats]", "executing job");
	var timestamp = (new Date()).toISOString();
	/*analytics.generateSolutionStats(timestamp, "true", config.refresh_interval, function(err, result) {
		if (err) {
			console.error((new Date()).toISOString(), "[jobs.refreshSolutionStats]", "ERROR: failed to refresh solution stats");
			console.error(err);
		} else {
			console.log(result);
			console.log((new Date()).toISOString(), "[jobs.refreshSolutionStats]", "solution stats refreshed");
		}
	});*/
	analytics.saveTimeSeries(timestamp.substring(0,4), null, true, config.cache, function(err, result) {
		if (err) {
			console.error((new Date()).toISOString(), "[jobs.refreshSolutionStats]", "ERROR: failed to refresh solution stats");
			console.error(err);
		} else {
			analytics.generateSolutionStats(timestamp, "true", function(err, result) {
				if (err) {
					console.error((new Date()).toISOString(), "[jobs.refreshSolutionStats]", "ERROR: failed to refresh solution stats");
					console.error(err);
				} else {
					console.log(result);
					console.log((new Date()).toISOString(), "[jobs.refreshSolutionStats]", "solution stats refreshed");
				}
			});
		}
	});
}

/**
 * Refresh Spotlight Solution Stats Job
 * Periodically refresh the Spotlight solution stats for the dashboard
 * @param {Object} config - JSON config properties
 */
function refreshSpotlightSolutionStats(config) {
	console.log((new Date()).toISOString(), "[jobs.refreshSpotlightSolutionStats]", "executing job");
	var timestamp = (new Date()).toISOString();
	analyticsSpotlight.saveSpotlightTimeSeries(timestamp.substring(0,4), null, true, config.cache, function(err, result) {
		if (err) {
			console.error((new Date()).toISOString(), "[jobs.refreshSpotlightSolutionStats]", "ERROR: failed to refresh Spotlight solution stats");
			console.error(err);
		} else {
			analyticsSpotlight.generateSpotlightSolutionStats(timestamp, "true", function(err, result) {
				if (err) {
					console.error((new Date()).toISOString(), "[jobs.refreshSpotlightSolutionStats]", "ERROR: failed to refresh Spotlight solution stats");
					console.error(err);
				} else {
					console.log(result);
					console.log((new Date()).toISOString(), "[jobs.refreshSpotlightSolutionStats]", "Spotlight solution stats refreshed");
				}
			});
		}
	});
}

/**
 * Refresh Modified Tickets Solution Stats Job
 * Periodically refresh the Modified Tickets solution stats for the dashboard
 * @param {Object} config - JSON config properties
 */
function refreshModifiedSolutionStats(config) {
	console.log((new Date()).toISOString(), "[jobs.refreshModifiedSolutionStats]", "executing job");
	var timestamp = (new Date()).toISOString();
	analyticsModified.saveModifiedTimeSeries(timestamp.substring(0,4), null, true, null, function(err, result) {
		if (err) {
			console.error((new Date()).toISOString(), "[jobs.refreshModifiedSolutionStats]", "ERROR: failed to refresh Modified Tickets solution stats");
			console.error(err);
		} else {
			analyticsModified.generateModifiedSolutionStats(timestamp, true, function(err, result) {
				if (err) {
					console.error((new Date()).toISOString(), "[jobs.refreshModifiedSolutionStats]", "ERROR: failed to refresh Modified Tickets solution stats");
					console.error(err);
				} else {
					console.log(result);
					console.log((new Date()).toISOString(), "[jobs.refreshModifiedSolutionStats]", "Modified Tickets solution stats refreshed");
				}
			});
		}
	});
}

/**
 * Refresh Tickets Updated KMM Data Job
 * Periodically refresh the tickets updated KMM data for use by reports and the dashboard
 * @param {Object} config - JSON config properties
 */
function refreshTicketsUpdatedKMMData(config) {
	console.log((new Date()).toISOString(), "[jobs.refreshTicketsUpdatedKMMData]", "executing job");
	var timestamp = (new Date()).toISOString();
	var startDates = [];
	var year = Number(timestamp.substring(0,4));
	var month = Number(timestamp.substring(5,7));
	for (i=(config.months); i>0; i--) {
		var start = year.toString() + "-"
		if (month < 10) start += "0"
		start += month.toString();
		startDates.push(start);
		if ((month - 1) < 1) {
			year--;
			month = 12;
		} else {
			month--;
		}
	}
	
	async.eachLimit(startDates, config.limit, function(start, callback) {
		console.log((new Date()).toISOString(), "[jobs.refreshTicketsUpdatedKMMData]", "executing task " + start);
		analytics.saveTicketsUpdatedKMM(start, function(err, result) {
			if (err) {
				callback(err);
			} else {
				console.log(result);
				console.log((new Date()).toISOString(), "[jobs.refreshTicketsUpdatedKMMData]", "finished task " + start);
				callback(null, result);
			}
		});
	}, function (err) {
		if (err) {
			console.error((new Date()).toISOString(), "[jobs.refreshTicketsUpdatedKMMData]", "ERROR: failed to refresh tickets updated KMM data");
			console.error(err);
		} else {
			console.log((new Date()).toISOString(), "[jobs.refreshTicketsUpdatedKMMData]", "tickets updated KMM data refreshed");
		}
	});
}

/**
 * Refresh Spotlight Tickets Updated KMM Data Job
 * Periodically refresh the tickets updated KMM data for use by reports and the dashboard
 * @param {Object} config - JSON config properties
 */
function refreshSpotlightTicketsUpdatedKMMData(config) {
	console.log((new Date()).toISOString(), "[jobs.refreshSpotlightTicketsUpdatedKMMData]", "executing job");
	var timestamp = (new Date()).toISOString();
	var startDates = [];
	var year = Number(timestamp.substring(0,4));
	var month = Number(timestamp.substring(5,7));
	for (i=(config.months); i>0; i--) {
		var start = year.toString() + "-"
		if (month < 10) start += "0"
		start += month.toString();
		startDates.push(start);
		if ((month - 1) < 1) {
			year--;
			month = 12;
		} else {
			month--;
		}
	}
	
	async.eachLimit(startDates, config.limit, function(start, callback) {
		console.log((new Date()).toISOString(), "[jobs.refreshSpotlightTicketsUpdatedKMMData]", "executing task " + start);
		analyticsSpotlight.saveSpotlightTicketsUpdatedKMM(start, function(err, result) {
			if (err) {
				callback(err);
			} else {
				console.log(result);
				console.log((new Date()).toISOString(), "[jobs.refreshSpotlightTicketsUpdatedKMMData]", "finished task " + start);
				callback(null, result);
			}
		});
	}, function (err) {
		if (err) {
			console.error((new Date()).toISOString(), "[jobs.refreshSpotlightTicketsUpdatedKMMData]", "ERROR: failed to refresh Spotlight tickets updated KMM data");
			console.error(err);
		} else {
			console.log((new Date()).toISOString(), "[jobs.refreshSpotlightTicketsUpdatedKMMData]", "Spotlight tickets updated KMM data refreshed");
		}
	});
}

/**
 * Save report to directory and write the new report log entry
 * @param {string} reportName
 * @param {string} reportDirectory
 * @param {string} timestamp
 * @param {string} reportLog
 * @param {string} maxEntries
 * @param {apiCallback} cb - callback that handles the response
 * @returns {Object} JSON
 */
function saveReport(reportData, reportName, reportDirectory, timestamp, reportLog, maxEntries, cb) {
	try {
		var zip = new JSZip();
		var fileName = reportName + "_" + timestamp.replace(/[-:\.]/g, "");
		zip.file(fileName  + ".txt", reportData);
		
		var buffer = zip.generate({type: "nodebuffer", compression: "DEFLATE"});
		
		fs.writeFile(path.join(__dirname, "public/reports/") + reportDirectory + "/" + fileName + ".zip", buffer, function(err) {
			if (err) return cb(err);
			analytics.saveReportLog(reportLog,
									maxEntries,
									reportDirectory,
									fileName + ".zip", timestamp,
			function(err, result) {
				if (err) return cb(err);
				cb(null, result);
			});
		});
	} catch(e) {
		cb(e);
	}
}

/**
 * Generate Tickets Updated KMM report for download
 * Based on the set schedule, generate the Tickets Updated KMM report and save it to the public/reports folder for download by brand focals
 * @param {string} type - type value for filtering tickets [all|hw|sw]
 * @param {Object} config - JSON config properties
 */
//var generateReportTicketsUpdatedKMM = function() {
function generateReportTicketsUpdatedKMM(type, config) {
	if (type && type === "") {
		type = "all";
	}
	console.log((new Date()).toISOString(), "[jobs.generateReportTicketsUpdatedKMM]", "executing job for type = ", type);
	var timestamp = (new Date()).toISOString();
	analytics.getTicketsUpdatedKMMCacheTxt(config.start_date, timestamp, "all", type, null, null, null, null, false, config.spotlight, config.format, function(err, result){
		if (err) {
			console.error((new Date()).toISOString(), "[jobs.generateReportTicketsUpdatedKMM]", "ERROR: failed to generate report");
			console.error(err);
		} else {
			saveReport(result, config.report_name, config.report_directory, timestamp, config.report_log, config.report_log_max_entries, function(err, result) {
				if (err) {
					console.error((new Date()).toISOString(), "[jobs.generateReportTicketsUpdatedKMM]", "ERROR: failed to save report");
					console.error(err);
				} else {
					console.log((new Date()).toISOString(), "[jobs.generateReportTicketsUpdatedKMM]", "report generated and saved");
				}
			});
		}
	});
}

/**
 * Generate Time Series report for download
 * Based on the set schedule, generate the Time Series report and save it to the public/reports folder for download by brand focals
 * @param {string} type - type value for filtering tickets [all|hw|sw]
 * @param {Object} config - JSON config properties
 */
//var generateReportTimeSeries = function() {
function generateReportTimeSeries(type, config) {
	if (type && type === "") {
		type = "all";
	}
	console.log((new Date()).toISOString(), "[jobs.generateReportTimeSeries]", "executing job for type = ", type);
	var timestamp = (new Date()).toISOString();
	analytics.generateTimeSeriesTxt(config.start_date, timestamp, null, type, false, config.cache, config.spotlight, function(err, result){
		if (err) {
			console.error((new Date()).toISOString(), "[jobs.generateReportTimeSeries]", "ERROR: failed to generate report");
			console.error(err);
		} else {
			saveReport(result, config.report_name, config.report_directory, timestamp, config.report_log, config.report_log_max_entries, function(err, result) {
				if (err) {
					console.error((new Date()).toISOString(), "[jobs.generateReportTimeSeries]", "ERROR: failed to save report");
					console.error(err);
				} else {
					console.log((new Date()).toISOString(), "[jobs.generateReportTimeSeries]", "report generated and saved");
				}
			});
		}
	});
}

/**
 * Generate Spotlight Tickets Updated KMM report for download
 * Based on the set schedule, generate the Spotlight Tickets Updated KMM report and save it to the public/reports folder for download by brand focals
 * @param {string} type - type value for filtering tickets [all|hw|sw]
 * @param {Object} config - JSON config properties
 */
function generateReportSpotlightTicketsUpdatedKMM(type, config) {
	if (type && type === "") {
		type = "all";
	}
	console.log((new Date()).toISOString(), "[jobs.generateReportSpotlightTicketsUpdatedKMM]", "executing job for type = ", type);
	var timestamp = (new Date()).toISOString();
	analyticsSpotlight.getSpotlightTicketsUpdatedKMMCacheTxt(config.start_date, timestamp, "all", type, null, null, null, null, false, config.format, function(err, result){
		if (err) {
			console.error((new Date()).toISOString(), "[jobs.generateReportSpotlightTicketsUpdatedKMM]", "ERROR: failed to generate report");
			console.error(err);
		} else {
			saveReport(result, config.report_name, config.report_directory, timestamp, config.report_log, config.report_log_max_entries, function(err, result) {
				if (err) {
					console.error((new Date()).toISOString(), "[jobs.generateReportSpotlightTicketsUpdatedKMM]", "ERROR: failed to save report");
					console.error(err);
				} else {
					console.log((new Date()).toISOString(), "[jobs.generateReportSpotlightTicketsUpdatedKMM]", "report generated and saved");
				}
			});
		}
	});
}

/**
 * Generate Spotlight Time Series report for download
 * Based on the set schedule, generate the Spotlight Time Series report and save it to the public/reports folder for download by brand focals
 * @param {string} type - type value for filtering tickets [all|hw|sw]
 * @param {Object} config - JSON config properties
 */
function generateReportSpotlightTimeSeries(type, config) {
	if (type && type === "") {
		type = "all";
	}
	console.log((new Date()).toISOString(), "[jobs.generateReportSpotlightTimeSeries]", "executing job for type = ", type);
	var timestamp = (new Date()).toISOString();
	analyticsSpotlight.generateSpotlightTimeSeriesTxt(config.start_date, timestamp, null, type, null, false, config.cahce, function(err, result){
		if (err) {
			console.error((new Date()).toISOString(), "[jobs.generateReportSpotlightTimeSeries]", "ERROR: failed to generate report");
			console.error(err);
		} else {
			saveReport(result, config.report_name, config.report_directory, timestamp, config.report_log, config.report_log_max_entries, function(err, result) {
				if (err) {
					console.error((new Date()).toISOString(), "[jobs.generateReportSpotlightTimeSeries]", "ERROR: failed to save report");
					console.error(err);
				} else {
					console.log((new Date()).toISOString(), "[jobs.generateReportSpotlightTimeSeries]", "report generated and saved");
				}
			});
		}
	});
}

/**
 * Generate Modified Tickets Time Series report for download
 * Based on the set schedule, generate the Modified Tickets Time Series report and save it to the public/reports folder for download by brand focals
 * @param {string} type - type value for filtering tickets [all|hw|sw]
 * @param {Object} config - JSON config properties
 */
function generateReportModifiedTimeSeries(type, config) {
	console.log((new Date()).toISOString(), "[jobs.generateReportModifiedTimeSeries]", "executing job for type = ", type);
	var timestamp = (new Date()).toISOString();
	analyticsModified.getModifiedTimeSeries(config.start_date, timestamp, type, "txt", null, false, function(err, result){
		if (err) {
			console.error((new Date()).toISOString(), "[jobs.generateReportModifiedTimeSeries]", "ERROR: failed to generate report");
			console.error(err);
		} else {
			saveReport(result, config.report_name, config.report_directory, timestamp, config.report_log, config.report_log_max_entries, function(err, result) {
				if (err) {
					console.error((new Date()).toISOString(), "[jobs.generateReportModifiedTimeSeries]", "ERROR: failed to save report");
					console.error(err);
				} else {
					console.log((new Date()).toISOString(), "[jobs.generateReportModifiedTimeSeries]", "report generated and saved");
				}
			});
		}
	});
}


/**
 * Generate Modified Tickets Updated KMM report for download
 * Based on the set schedule, generate the Modified Tickets Updated KMM report and save it to the public/reports folder for download by brand focals
 * @param {string} type - type value for filtering tickets [all|hw|sw]
 * @param {Object} config - JSON config properties
 */
function generateReportModifiedTicketsUpdatedKMM(type, config) {
	console.log((new Date()).toISOString(), "[jobs.generateReportModifiedTicketsUpdatedKMM]", "executing job for type = ", type);
	var timestamp = (new Date()).toISOString();
	analyticsModified.getModifiedTicketsUpdatedKMM(config.start_date, timestamp, null, type, "all", "txt", null, null, null, null, false, function(err, result){
		if (err) {
			console.error((new Date()).toISOString(), "[jobs.generateReportModifiedTicketsUpdatedKMM]", "ERROR: failed to generate report");
			console.error(err);
		} else {
			saveReport(result, config.report_name, config.report_directory, timestamp, config.report_log, config.report_log_max_entries, function(err, result) {
				if (err) {
					console.error((new Date()).toISOString(), "[jobs.generateReportModifiedTicketsUpdatedKMM]", "ERROR: failed to save report");
					console.error(err);
				} else {
					console.log((new Date()).toISOString(), "[jobs.generateReportModifiedTicketsUpdatedKMM]", "report generated and saved");
				}
			});
		}
	});
}

/**
 * Start all the jobs
 */
function startJobs(cb) {
	try {
		for (key in jobs) {
			if (jobsConfig[key] && jobsConfig[key].on) {
				jobs[key].start();
				console.log((new Date()).toISOString(), "[jobs.startJobs] - job started", key);
			}
		}
		console.log((new Date()).toISOString(), "[jobs.startJobs]", "jobs are started");
		
		cb(null, {status: "started", jobs_config: jobsConfig});
	} catch(e) {
		cb(e);
	}
}

/**
 * Stop all the jobs
 */
function stopJobs(cb) {
	try {
		for (key in jobs) {
			jobs[key].stop();
			console.log((new Date()).toISOString(), "[jobs.stopJobs] - job stopped", key);
		}
		console.log((new Date()).toISOString(), "[jobs.stopJobs]", "jobs are stopped");
		
		cb(null, {status: "stopped", jobs_config: jobsConfig})
	} catch(e) {
		cb(e);
	}
}

/**
 * Normalize a port into a number, string, or false.
 */
function normalizePort(val) {
  var port = parseInt(val, 10);

  if (isNaN(port)) {
    // named pipe
    return val;
  }

  if (port >= 0) {
    // port number
    return port;
  }

  return false;
}

/**
 * Execute Start Jobs API
 * Allows for manual start/stop of jobs
 */
exports.executeStartJobs = function(req, res) {
	startJobs(function(err, result) {
		if (err) {
			console.error((new Date()).toISOString(), "[jobs.executeStartJobs]", "ERROR: failed to start jobs");
			console.error(err);
			res.status(500).send("Error occurred, failed to start jobs");
		} else {
			res.json(result);
		}
	});
}

/**
 * Execute Stop Jobs API
 * Allows for manual start/stop of jobs
 */
exports.executeStopJobs = function(req, res) {
	stopJobs(function(err, result) {
		if (err) {
			console.error((new Date()).toISOString(), "[jobs.executeStopJobs]", "ERROR: failed to stop jobs");
			console.error(err);
			res.status(500).send("Error occurred, failed to stop jobs");
		} else {
			res.json(result);
		}
	});
}