/*jshint esnext:true*/
/*globals module:true, require:true, process:true*/

/*
 * Grunt Task File
 * ---------------
 *
 * Task: S3
 * Description: Move files to and from s3
 * Dependencies: knox, async, underscore.deferred
 *
 */

module.exports = function (grunt) {

  /**
   * Module dependencies.
   */

  // Core.
  const util = require('util');
  const crypto = require('crypto');
  const fs = require('fs');
  const path = require('path');
  const url = require('url');
  const zlib = require('zlib');

  // Npm.
  const knox = require('knox');
	const iniparser = require('iniparser');
  const mime = require('mime');
  const async = require('async');
  const _ = require('underscore');
  const deferred = require('underscore.deferred');
  _.mixin(deferred);

  const existsSync = ('existsSync' in fs) ? fs.existsSync : path.existsSync;

  /**
   * Grunt aliases.
   */
  var log = grunt.log;

  /**
   * Success/error messages.
   */
  const MSG_UPLOAD_SUCCESS = '↗'.blue + ' Uploaded: %s (%s)';
  const MSG_DOWNLOAD_SUCCESS = '↙'.yellow + ' Downloaded: %s (%s)';
  const MSG_DELETE_SUCCESS = '✗'.red + ' Deleted: %s';
  const MSG_COPY_SUCCESS = '→'.cyan + ' Copied: %s to %s';

  const MSG_ERR_NOT_FOUND = '¯\\_(ツ)_/¯ File not found: %s';
  const MSG_ERR_UPLOAD = 'Upload error: %s (%s)';
  const MSG_ERR_DOWNLOAD = 'Download error: %s (%s)';
  const MSG_ERR_DELETE = 'Delete error: %s (%s)';
  const MSG_ERR_COPY = 'Copy error: %s to %s';
  const MSG_ERR_CHECKSUM = 'Expected hash: %s but found %s for %s';

  /**
   * Create an Error object based off of a formatted message. Arguments
   * are identical to those of util.format.
   *
   * @param {String} Format.
   * @param {...string|number} Values to insert into Format.
   * @returns {Error}
   */
  function makeError () {
    var msg = util.format.apply(util, _.toArray(arguments));
    return new Error(msg);
  }

  /**
   * Get the grunt s3 configuration options, filling in options from
   * environment variables if present. Also supports grunt template strings.
   *
   * @returns {Object} The s3 configuration.
   */
  function getConfig () {
    var dfd = _.Deferred();

    // FIXME: skip reading from .s3cfg entirely

    var config = grunt.config.process('s3') || {};

    // Look for and process grunt template stings
    var keys = ['key', 'secret', 'bucket'];
    keys.forEach(function(key) {
      if (config.hasOwnProperty(key)) {
        config[key] = grunt.template.process(config[key]);
      }
    });

    config = _.defaults(config, {
      key : process.env.AWS_ACCESS_KEY_ID,
      secret : process.env.AWS_SECRET_ACCESS_KEY,
      upload: [],
      download: [],
      del: [],
      copy: []
    });

    if(config.key && config.secret) {
      dfd.resolve(config);
    } else {
      // Try to read ~/.s3cfg
      iniparser.parse(path.join(process.env['HOME'], '.s3cfg'), function(err, iniconfig) {
        iniconfig = iniconfig['default'];

        if(iniconfig['access_key']) config['key'] = iniconfig['access_key'];
        if(iniconfig['secret_key']) config['secret'] = iniconfig['secret_key'];

        if(config['key'] && config['secret']) {
          dfd.resolve(config);
        } else {
          dfd.reject();
        }
      });
    }

    return dfd;
  }

  /**
   * Transfer files to/from s3.
   *
   * Uses global s3 grunt config.
   */
  grunt.registerTask('s3', 'Publishes files to s3.', function () {
    var done = this.async();

    getConfig()
      .then(function(config) {

        var transfers = [];

        config.upload.forEach(function(upload) {

          _.defaults(upload, _.pick(config, [ 'endpoint', 'port', 'key', 'secret', 'access', 'bucket' ]));

          var processFile = function(file, err, stat) {
            var files = [];

            if(stat.isDirectory()) {
              // .recurse() is synchronous
              grunt.file.recurse(file, function(abspath, rootdir, subdir, filename) {
                var dest = [ upload.dest, subdir ?  subdir : path.basename(rootdir, '/') ];

                files.push({
                  src: path.join(rootdir, subdir, filename),
                  dest: path.join.apply(null, dest)
                });
              });
            } else {
              files = _.map(grunt.file.expandFiles(file), function(file) {
                return {
                  src: file, dest: upload.dest
                }
              })
            }

            files.forEach(function(file) {
              // If there is only 1 file and it matches the original file wildcard,
              // we know this is a single file transfer. Otherwise, we need to build
              // the destination.
              var dest = (files.length === 1 && file.src === upload.src) ?
                file.dest :
                path.join(file.dest, path.basename(file.src));

              transfers.push(grunt.helper('s3.put', file.src, dest, upload));
            });
          };

          var files = grunt.file.expand(upload.src);

          if(!files.length) files = [ upload.src ];

          for(var i = 0; i < files.length; i++) {
            var stat = fs.statSync(files[i]);
            processFile(files[i], null, stat);
          }

        });

        config.download.forEach(function(download) {
          _.defaults(download, _.pick(config, [ 'endpoint', 'port', 'key', 'secret', 'access', 'bucket' ]));

          transfers.push(grunt.helper('s3.pull', download.src, download.dest, download));
        });

        config.del.forEach(function(del) {
          _.defaults(del, _.pick(config, [ 'endpoint', 'port', 'key', 'secret', 'access', 'bucket' ]));

          transfers.push(grunt.helper('s3.delete', del.src, del));
        });

        config.copy.forEach(function(copy) {
          _.defaults(copy, _.pick(config, [ 'endpoint', 'port', 'key', 'secret', 'access', 'bucket' ]));

          transfers.push(grunt.helper('s3.copy', copy.src, copy.dest, copy));
        });

        var total = transfers.length;
        var errors = 0;

        // Keep a running total of errors/completions as the transfers complete.
        transfers.forEach(function(transfer) {
          transfer.done(function(msg) {
            log.ok(msg);
          });

          transfer.fail(function(msg) {
            log.error(msg);
            ++errors;
          });

          transfer.always(function() {
            // If this was the last transfer to complete, we're all done.
            if (--total === 0) {
              done(!errors);
            }
          });
        });

      });
  });

  /**
   * Publishes the local file at src to the s3 dest.
   *
   * Verifies that the upload was successful by comparing an md5 checksum of
   * the local and remote versions.
   *
   * @param {String} src The local path to the file to upload.
   * @param {String} dest The s3 path, relative to the bucket, to which the src
   *     is uploaded.
   * @param {Object} [options] An object containing options which override any
   *     option declared in the global s3 config.
   */
  grunt.registerHelper('s3.put', function (src, dest, options) {
    var dfd = new _.Deferred();

    // Make sure the local file exists.
    if (!existsSync(src)) {
      return dfd.reject(makeError(MSG_ERR_NOT_FOUND, src));
    }

    // var config = _.defaults(options || {}, getConfig());
    var headers = options.headers || {};

    if (options.access) {
      headers['x-amz-acl'] = options.access;
    }

    // Pick out the configuration options we need for the client.
    var client = knox.createClient(_(options).pick([
      'endpoint', 'port', 'key', 'secret', 'access', 'bucket'
    ]));

    // Encapsulate this logic to make it easier to gzip the file first if
    // necesssary.
    function upload(cb) {
      cb = cb || function () {};

      // Upload the file to s3.
      client.putFile(src, dest, headers, function (err, res) {
        // If there was an upload error or any status other than a 200, we
        // can assume something went wrong.
        if (err || res.statusCode !== 200) {
          cb(makeError(MSG_ERR_UPLOAD, src, err || res.statusCode));
        }
        else {
          // Read the local file so we can get its md5 hash.
          fs.readFile(src, function (err, data) {
            if (err) {
              cb(makeError(MSG_ERR_UPLOAD, src, err));
            }
            else {
              // The etag head in the response from s3 has double quotes around
              // it. Strip them out.
              var remoteHash = res.headers.etag.replace(/"/g, '');

              // Get an md5 of the local file so we can verify the upload.
              var localHash = crypto.createHash('md5').update(data).digest('hex');

              if (remoteHash === localHash) {
                var msg = util.format(MSG_UPLOAD_SUCCESS, src, localHash);
                cb(null, msg);
              }
              else {
                cb(makeError(MSG_ERR_CHECKSUM, localHash, remoteHash, src));
              }
            }
          });
        }
      });
    }

    // If gzip is enabled, gzip the file into a temp file and then perform the
    // upload. The `gzip` option can be a function, that is passed the file path.

    var gzip = _.isFunction(options.gzip) ?  options.gzip(src) : gzip;
    if (gzip) {
      headers['Content-Encoding'] = 'gzip';
      headers['Content-Type'] = mime.lookup(src);

      // Determine a unique temp file name.
      var tmp = src + '.gz';
      var incr = 0;
      while (existsSync(tmp)) {
        tmp = src + '.' + (incr++) + '.gz';
      }

      var input = fs.createReadStream(src);
      var output = fs.createWriteStream(tmp);

      // Gzip the file and upload when done.
      input.pipe(zlib.createGzip()).pipe(output)
        .on('error', function (err) {
          dfd.reject(makeError(MSG_ERR_UPLOAD, src, err));
        })
        .on('close', function () {
          // Update the src to point to the newly created .gz file.
          src = tmp;
          upload(function (err, msg) {
            // Clean up the temp file.
            fs.unlinkSync(tmp);

            if (err) {
              dfd.reject(err);
            }
            else {
              dfd.resolve(msg);
            }
          });
        });
    }
    else {
      // No need to gzip so go ahead and upload the file.
      upload(function (err, msg) {
        if (err) {
          dfd.reject(err);
        }
        else {
          dfd.resolve(msg);
        }
      });
    }

    return dfd;
  });

  /**
   * Download a file from s3.
   *
   * Verifies that the download was successful by downloading the file and
   * comparing an md5 checksum of the local and remote versions.
   *
   * @param {String} src The s3 path, relative to the bucket, of the file being
   *     downloaded.
   * @param {String} dest The local path where the download will be saved.
   * @param {Object} [options] An object containing options which override any
   *     option declared in the global s3 config.
   */
  grunt.registerHelper('s3.pull', function (src, dest, options) {
    var dfd = new _.Deferred();

    // Create a local stream we can write the downloaded file to.
    var file = fs.createWriteStream(dest);

    // Pick out the configuration options we need for the client.
    var client = knox.createClient(_(options).pick([
      'endpoint', 'port', 'key', 'secret', 'access', 'bucket'
    ]));

    // Upload the file to s3.
    client.getFile(src, function (err, res) {
      // If there was an upload error or any status other than a 200, we
      // can assume something went wrong.
      if (err || res.statusCode !== 200) {
        return dfd.reject(makeError(MSG_ERR_DOWNLOAD, src, err || res.statusCode));
      }

      res
        .on('data', function (chunk) {
          file.write(chunk);
        })
        .on('error', function (err) {
          return dfd.reject(makeError(MSG_ERR_DOWNLOAD, src, err));
        })
        .on('end', function () {
          file.end();

          // Read the local file so we can get its md5 hash.
          fs.readFile(dest, function (err, data) {
            if (err) {
              return dfd.reject(makeError(MSG_ERR_DOWNLOAD, src, err));
            }
            else {
              // The etag head in the response from s3 has double quotes around
              // it. Strip them out.
              var remoteHash = res.headers.etag.replace(/"/g, '');

              // Get an md5 of the local file so we can verify the download.
              var localHash = crypto.createHash('md5').update(data).digest('hex');

              if (remoteHash === localHash) {
                var msg = util.format(MSG_DOWNLOAD_SUCCESS, src, localHash);
                dfd.resolve(msg);
              }
              else {
                dfd.reject(makeError(MSG_ERR_CHECKSUM, localHash, remoteHash, src));
              }
            }
          });
        });
    });

    return dfd;
  });

  /**
   * Copy a file from s3 to s3.
   *
   * @param {String} src The s3 path, including the bucket, to the file to
   *     copy.
   * @param {String} dest The s3 path, relative to the bucket, to the file to
   *     create.
   * @param {Object} [options] An object containing options which override any
   *     option declared in the global s3 config.
   */
  grunt.registerHelper('s3.copy', function (src, dest, options) {
    var dfd = new _.Deferred();

    // Pick out the configuration options we need for the client.
    var client = knox.createClient(_(options).pick([
      'endpoint', 'port', 'key', 'secret', 'access', 'bucket'
    ]));

    // Copy the src file to dest.
    var req = client.put(dest, {
      'Content-Length': 0,
      'x-amz-copy-source' : src
    });

    req.on('response', function (res) {
      if (res.statusCode !== 200) {
        dfd.reject(makeError(MSG_ERR_COPY, src, dest));
      }
      else {
        dfd.resolve(util.format(MSG_COPY_SUCCESS, src, dest));
      }
    });

    return dfd;
  });

  /**
   * Delete a file from s3.
   *
   * @param {String} src The s3 path, relative to the bucket, to the file to
   *     delete.
   * @param {Object} [options] An object containing options which override any
   *     option declared in the global s3 config.
   */
  grunt.registerHelper('s3.delete', function (src, options) {
    var dfd = new _.Deferred();

    // Pick out the configuration options we need for the client.
    var client = knox.createClient(_(options).pick([
      'endpoint', 'port', 'key', 'secret', 'access', 'bucket'
    ]));

    // Upload the file to this endpoint.
    client.deleteFile(src, function (err, res) {
      if (err || res.statusCode !== 204) {
        dfd.reject(makeError(MSG_ERR_DELETE, src, err || res.statusCode));
      }
      else {
        dfd.resolve(util.format(MSG_DELETE_SUCCESS, src));
      }
    });

    return dfd;
  });

};

