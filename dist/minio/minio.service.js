"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.MinioStorageService = void 0;
const common_1 = require("@nestjs/common");
const config_1 = require("@nestjs/config");
const event_emitter_1 = require("@nestjs/event-emitter");
const minio_1 = require("minio");
const file_queue_service_1 = require("../file-queue.service");
const constant_1 = require("../utils/constant");
const stream_1 = require("stream");
const zlib = require("zlib");
const util_1 = require("util");
let MinioStorageService = class MinioStorageService {
    constructor(configService, fileQueueService, eventEmitter) {
        this.configService = configService;
        this.fileQueueService = fileQueueService;
        this.eventEmitter = eventEmitter;
        this.errors = [];
        this.gzip = (0, util_1.promisify)(zlib.gzip);
        this.gunzip = (0, util_1.promisify)(zlib.gunzip);
        this.deflate = (0, util_1.promisify)(zlib.deflate);
        this.inflate = (0, util_1.promisify)(zlib.inflate);
        const bucket = this.configService.get('APP_NAME');
        const endPoint = this.configService.get('MINIO_ENDPOINT');
        const port = this.configService.get('MINIO_PORT');
        const useSSL = this.configService.get('MINIO_USE_SSL');
        const accessKey = this.configService.get('MINIO_ACCESS_KEY');
        const secretKey = this.configService.get('MINIO_SECRET_KEY');
        const missingVars = [];
        if (!bucket)
            missingVars.push('APP_NAME');
        if (!endPoint)
            missingVars.push('MINIO_ENDPOINT');
        if (!port)
            missingVars.push('MINIO_PORT');
        if (!accessKey)
            missingVars.push('MINIO_ACCESS_KEY');
        if (!secretKey)
            missingVars.push('MINIO_SECRET_KEY');
        if (missingVars.length > 0) {
            this.errors.push(`Missing required environment variables: ${missingVars.join(', ')}. Please check the README for configuration details.`);
        }
        else {
            const validBuckets = ['working-space', 'e-health', 'chat', 'avatar'];
            if (!validBuckets.includes(bucket)) {
                this.errors.push(`Invalid bucket name: ${bucket}. Must be one of: ${validBuckets.join(', ')}. Please check the README for configuration details.`);
            }
            else {
                this.bucket = bucket;
            }
            if (this.errors.length === 0) {
                try {
                    this.minioClient = new minio_1.Client({
                        endPoint,
                        port: parseInt(port, 10),
                        useSSL: useSSL === 'true',
                        accessKey,
                        secretKey,
                    });
                    this.testConnection();
                }
                catch (error) {
                    this.errors.push(`Failed to initialize MinIO client: ${error.message}. Please check the README for configuration details.`);
                    this.minioClient = null;
                }
            }
            else {
                this.minioClient = null;
            }
        }
        this.setupEventListeners();
    }
    async testConnection() {
        if (!this.minioClient)
            return;
        try {
            await this.minioClient.listBuckets();
            console.log('Successfully connected to MinIO server.');
        }
        catch (error) {
            this.errors.push(`MinIO connection test failed: ${error.message}. Please check the README for configuration details.`);
            this.minioClient = null;
        }
    }
    setupEventListeners() {
        this.eventEmitter.on('upload.completed', ({ taskId, result }) => {
            console.log(`Upload completed for task ${taskId}:`, result);
        });
        this.eventEmitter.on('upload.failed', ({ taskId, error }) => {
            console.error(`Upload failed for task ${taskId}:`, error);
        });
        this.eventEmitter.on('upload.progress', ({ taskId, progress }) => {
            console.log(`Upload progress for task ${taskId}: ${progress}%`);
        });
        this.eventEmitter.on('download.completed', ({ taskId, result }) => {
            console.log(`Download completed for task ${taskId}`);
        });
        this.eventEmitter.on('download.failed', ({ taskId, error }) => {
            console.error(`Download failed for task ${taskId}:`, error);
        });
    }
    async onModuleInit() {
        if (this.errors.length > 0) {
            console.error('MinIO initialization errors:');
            this.errors.forEach((error, index) => {
                console.error(`[${index + 1}] ${error}`);
            });
        }
        if (!this.minioClient || !this.bucket) {
            console.error('MinIO client or bucket not initialized. Skipping bucket setup.');
            return;
        }
        try {
            const bucketExists = await this.minioClient.bucketExists(this.bucket);
            if (!bucketExists) {
                console.log(`Bucket ${this.bucket} does not exist. Creating it...`);
                await this.minioClient.makeBucket(this.bucket, 'us-east-1');
                console.log(`Bucket ${this.bucket} created successfully.`);
            }
            else {
                console.log(`Bucket ${this.bucket} already exists.`);
            }
        }
        catch (error) {
            console.error(`Failed to initialize bucket ${this.bucket}: ${error.message}. Please check the README for configuration details.`);
        }
    }
    isConnected() {
        return !!this.minioClient;
    }
    getFileExtension(fileName) {
        return fileName.split('.').pop()?.toLowerCase() || '';
    }
    getDatePrefix() {
        const date = new Date();
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        return `${year}-${month}-${day}`;
    }
    generateFilePath(fileName) {
        const extension = this.getFileExtension(fileName);
        const datePrefix = this.getDatePrefix();
        return `${this.bucket}/${datePrefix}/${extension}/${fileName}`;
    }
    parsePathMinio(fullPath) {
        const [bucket, ...rest] = fullPath.split('/');
        const path = rest.join('/');
        return { bucket, path };
    }
    async ensureBucketExists(bucket) {
        const exists = await this.minioClient.bucketExists(bucket);
        if (!exists) {
            await this.minioClient.makeBucket(bucket);
        }
    }
    validateFileSize(buffer) {
        if (typeof buffer === 'string') {
            return;
        }
        if (buffer instanceof Buffer) {
            const fileSize = buffer.length;
            if (fileSize > constant_1.MAX_FILE_SIZE) {
                throw new Error(`File size ${fileSize} exceeds maximum allowed size of ${constant_1.MAX_FILE_SIZE} bytes (${constant_1.MAX_FILE_SIZE / (1024 * 1024)}MB)`);
            }
        }
    }
    async handleUpload(data) {
        await this.ensureBucketExists(this.bucket);
        try {
            this.validateFileSize(data.buffer);
            const pathFileUpload = await this.fileQueueService.addUploadJob(data, this.uploadHandler.bind(this));
            return pathFileUpload;
        }
        catch (error) {
            this.eventEmitter.emit('upload.failed', {
                taskId: data.fileName,
                error: error.message
            });
            throw error;
        }
    }
    async upload(data) {
        return this.handleUpload(data);
    }
    validateImageFile(fileName) {
        const allowedExtensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp'];
        const lowerFileName = fileName.toLowerCase();
        const isValid = allowedExtensions.some(ext => lowerFileName.endsWith(ext));
        if (!isValid) {
            throw new Error('Only image files are allowed for avatars.');
        }
    }
    async uploadAvatar(data) {
        this.bucket = 'avatar';
        this.validateImageFile(data.fileName);
        return this.handleUpload(data);
    }
    async uploadHandler(data) {
        const { fileName, mimeType, buffer } = data;
        const metaData = {
            'Content-Type': mimeType,
        };
        const newFilePath = this.generateFilePath(fileName);
        const minioPath = newFilePath.replace(`${this.bucket}/`, '');
        data.fileName = minioPath;
        try {
            if (buffer.length < constant_1.MULTIPART_THRESHOLD) {
                await this.minioClient.putObject(this.bucket, minioPath, buffer, buffer.length, metaData);
            }
            else {
                const bufferStream = new stream_1.PassThrough();
                bufferStream.end(buffer);
                await this.minioClient.putObject(this.bucket, minioPath, bufferStream, buffer.length, metaData);
            }
            console.log(`Đã upload ${minioPath} (${buffer.length} bytes)`);
            return newFilePath;
        }
        catch (error) {
            console.error('Lỗi khi upload lên Minio:', error);
            throw error;
        }
    }
    async getFileInfo(fullPath) {
        const { bucket, path } = this.parsePathMinio(fullPath);
        try {
            return await this.minioClient.statObject(bucket, path);
        }
        catch (error) {
            throw new common_1.NotFoundException(`Đường dẫn ${fullPath} không tồn tại`);
        }
    }
    async download(fullPath) {
        try {
            const { path, bucket } = this.parsePathMinio(fullPath);
            return await this.minioClient.getObject(bucket, path);
        }
        catch (error) {
            throw new common_1.NotFoundException(`Đường dẫn ${fullPath} không tồn tại`);
        }
    }
    async getBuffer(fullPath) {
        try {
            const { path, bucket } = this.parsePathMinio(fullPath);
            const fileInfo = await this.getFileInfo(path);
            if (fileInfo.size > constant_1.BUFFER_THRESHOLD) {
                throw new common_1.BadRequestException(`File size (${fileInfo.size} bytes) exceeds buffer threshold (${constant_1.BUFFER_THRESHOLD} bytes)`);
            }
            const fileStream = await this.minioClient.getObject(bucket, path);
            const chunks = [];
            return new Promise((resolve, reject) => {
                fileStream.on('data', (chunk) => chunks.push(chunk));
                fileStream.on('end', () => resolve(Buffer.concat(chunks)));
                fileStream.on('error', (err) => reject(err));
            });
        }
        catch (error) {
            if (error instanceof common_1.NotFoundException || error instanceof common_1.BadRequestException) {
                throw error;
            }
            console.error('Lỗi khi lấy buffer từ Minio:', error);
            throw new common_1.NotFoundException(`Đường dẫn ${fullPath} không tồn tại`);
        }
    }
    async compressBuffer(buffer) {
        try {
            return await this.gzip(buffer);
        }
        catch (error) {
            console.error('Lỗi khi nén buffer:', error);
            throw new common_1.BadRequestException('Không thể nén file');
        }
    }
    async decompressBuffer(compressedBuffer) {
        try {
            return await this.gunzip(compressedBuffer);
        }
        catch (error) {
            console.error('Lỗi khi giải nén buffer:', error);
            throw new common_1.BadRequestException('Không thể giải nén file');
        }
    }
    async uploadCompressed(data) {
        try {
            console.log(`Bắt đầu nén file ${data.fileName} (${data.buffer.length} bytes)`);
            const compressedBuffer = await this.compressBuffer(data.buffer);
            const compressionRatio = ((data.buffer.length - compressedBuffer.length) / data.buffer.length * 100).toFixed(2);
            console.log(`File đã được nén từ ${data.buffer.length} bytes xuống ${compressedBuffer.length} bytes (tiết kiệm ${compressionRatio}%)`);
            const compressedData = {
                ...data,
                buffer: compressedBuffer,
                fileName: data.fileName + '.gz',
                mimeType: 'application/gzip'
            };
            return await this.handleUpload(compressedData);
        }
        catch (error) {
            console.error('Lỗi khi upload file đã nén:', error);
            throw error;
        }
    }
    async uploadDeflateCompressed(data) {
        try {
            console.log(`Bắt đầu nén file ${data.fileName} bằng DEFLATE (${data.buffer.length} bytes)`);
            const compressedBuffer = await this.deflate(data.buffer);
            const compressionRatio = ((data.buffer.length - compressedBuffer.length) / data.buffer.length * 100).toFixed(2);
            console.log(`File đã được nén từ ${data.buffer.length} bytes xuống ${compressedBuffer.length} bytes (tiết kiệm ${compressionRatio}%)`);
            const compressedData = {
                ...data,
                buffer: compressedBuffer,
                fileName: data.fileName + '.deflate',
                mimeType: 'application/deflate'
            };
            return await this.handleUpload(compressedData);
        }
        catch (error) {
            console.error('Lỗi khi upload file đã nén bằng DEFLATE:', error);
            throw error;
        }
    }
    async downloadAndDecompress(fullPath) {
        try {
            console.log(`Bắt đầu download và giải nén file: ${fullPath}`);
            const compressedBuffer = await this.getBuffer(fullPath);
            console.log(`Đã download file nén (${compressedBuffer.length} bytes)`);
            const decompressedBuffer = await this.decompressBuffer(compressedBuffer);
            console.log(`File đã được giải nén thành ${decompressedBuffer.length} bytes`);
            return decompressedBuffer;
        }
        catch (error) {
            console.error('Lỗi khi download và giải nén file:', error);
            throw error;
        }
    }
    async downloadAndInflate(fullPath) {
        try {
            console.log(`Bắt đầu download và giải nén file DEFLATE: ${fullPath}`);
            const compressedBuffer = await this.getBuffer(fullPath);
            console.log(`Đã download file nén DEFLATE (${compressedBuffer.length} bytes)`);
            const decompressedBuffer = await this.inflate(compressedBuffer);
            console.log(`File đã được giải nén thành ${decompressedBuffer.length} bytes`);
            return decompressedBuffer;
        }
        catch (error) {
            console.error('Lỗi khi download và giải nén file DEFLATE:', error);
            throw error;
        }
    }
    async downloadCompressedStream(fullPath) {
        try {
            console.log(`Bắt đầu download stream và giải nén: ${fullPath}`);
            const compressedStream = await this.download(fullPath);
            const gunzipStream = zlib.createGunzip();
            return compressedStream.pipe(gunzipStream);
        }
        catch (error) {
            console.error('Lỗi khi download compressed stream:', error);
            throw error;
        }
    }
    isCompressedFile(fullPath) {
        const lowerPath = fullPath.toLowerCase();
        return lowerPath.endsWith('.gz') || lowerPath.endsWith('.deflate') || lowerPath.endsWith('.gzip');
    }
    async smartDownload(fullPath) {
        try {
            if (this.isCompressedFile(fullPath)) {
                console.log(`Phát hiện file nén, tiến hành giải nén: ${fullPath}`);
                if (fullPath.toLowerCase().endsWith('.deflate')) {
                    return await this.downloadAndInflate(fullPath);
                }
                else {
                    return await this.downloadAndDecompress(fullPath);
                }
            }
            else {
                console.log(`File không nén, download bình thường: ${fullPath}`);
                return await this.getBuffer(fullPath);
            }
        }
        catch (error) {
            console.error('Lỗi khi smart download:', error);
            throw error;
        }
    }
    async uploadWithAutoCompression(data, compressionThreshold = 1024 * 1024) {
        try {
            if (data.buffer.length > compressionThreshold) {
                console.log(`File ${data.fileName} lớn hơn ${compressionThreshold} bytes, tiến hành nén...`);
                return await this.uploadCompressed(data);
            }
            else {
                console.log(`File ${data.fileName} nhỏ hơn ${compressionThreshold} bytes, upload bình thường...`);
                return await this.upload(data);
            }
        }
        catch (error) {
            console.error('Lỗi khi upload với auto compression:', error);
            throw error;
        }
    }
    async generatePresignedUrl(objectName, method = 'GET', expiry = 3600) {
        return await this.minioClient.presignedUrl(method, this.bucket, objectName, expiry);
    }
};
exports.MinioStorageService = MinioStorageService;
exports.MinioStorageService = MinioStorageService = __decorate([
    (0, common_1.Injectable)(),
    __metadata("design:paramtypes", [config_1.ConfigService,
        file_queue_service_1.FileQueueService,
        event_emitter_1.EventEmitter2])
], MinioStorageService);
//# sourceMappingURL=minio.service.js.map