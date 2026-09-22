const fs = require('fs-extra');
const path = require('path');
const { S3Client, PutObjectCommand, HeadObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const config = require('../config');
const { logger } = require('./state');

let client;

function getClient() {
  if (client) return client;
  const r2 = config.r2;
  if (!r2.endpoint || !r2.accessKeyId || !r2.secretAccessKey || !r2.bucket) {
    throw new Error(
      'R2 is not configured. Set R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY and R2_BUCKET.'
    );
  }
  client = new S3Client({
    region: 'auto',
    endpoint: r2.endpoint,
    credentials: {
      accessKeyId: r2.accessKeyId,
      secretAccessKey: r2.secretAccessKey,
    },
  });
  return client;
}

function objectKey(id, filename) {
  const prefix = config.r2.prefix ? String(config.r2.prefix).replace(/^\/+|\/+$/g, '') : '';
  return [prefix, id, filename].filter(Boolean).join('/');
}

function publicUrl(key) {
  const base = config.r2.publicBaseUrl;
  if (!base) throw new Error('R2_PUBLIC_BASE_URL is not set (e.g. https://videos.filimehome.com)');
  return `${base}/${key}`;
}

async function headExists(key) {
  const c = getClient();
  try {
    await c.send(new HeadObjectCommand({ Bucket: config.r2.bucket, Key: key }));
    return true;
  } catch {
    return false;
  }
}

/**
 * Upload a file to R2. Skips upload when the object already exists unless
 * `forceUpload` is true. Returns the public URL.
 */
async function uploadFile({ id, filename, filePath, contentType = 'video/mp4', forceUpload = false }) {
  const key = objectKey(id, filename);

  if (!forceUpload) {
    const exists = await headExists(key);
    if (exists) {
      logger.info(`  already on R2 (${key}) — skipping upload`);
      return publicUrl(key);
    }
  }

  const body = fs.createReadStream(filePath);
  const size = (await fs.stat(filePath)).size;

  await getClient().send(
    new PutObjectCommand({
      Bucket: config.r2.bucket,
      Key: key,
      Body: body,
      ContentType: contentType,
      ContentLength: size,
      CacheControl: 'public, max-age=31536000, immutable',
    })
  );

  logger.info(`  uploaded ${key} (${Math.round(size / 1024 / 1024)} MB)`);
  return publicUrl(key);
}

/** Delete an object from R2 (used when evicting a movie to free budget). */
async function deleteObject(key) {
  const c = getClient();
  try {
    await c.send(new DeleteObjectCommand({ Bucket: config.r2.bucket, Key: key }));
    logger.info(`  deleted from R2: ${key}`);
    return true;
  } catch (err) {
    logger.warn(`  could not delete ${key}: ${err.message}`);
    return false;
  }
}

module.exports = { uploadFile, publicUrl, objectKey, deleteObject };