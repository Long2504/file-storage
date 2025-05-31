import { BadRequestException, Injectable, NotFoundException, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { Client } from 'minio';
import { FileQueueService } from 'src/file-queue.service';
import { BucketMinio, IFile } from 'src/interfaces/file.interface';

import { BUFFER_THRESHOLD, MAX_FILE_SIZE, MULTIPART_THRESHOLD } from 'src/utils/constant';
import { PassThrough } from 'stream';
import * as zlib from 'zlib';
import { promisify } from 'util';


@Injectable()
export class MinioStorageService implements OnModuleInit {
    private minioClient: Client | null;
    private bucket: BucketMinio | null;
    private readonly errors: string[] = []; // Array to store errors from constructor
    
    // Promisify zlib methods for async/await support
    private gzip = promisify(zlib.gzip);
    private gunzip = promisify(zlib.gunzip);
    private deflate = promisify(zlib.deflate);
    private inflate = promisify(zlib.inflate);

    constructor(
        private configService: ConfigService,
        private fileQueueService: FileQueueService,
        private eventEmitter: EventEmitter2
    ) {
        // Retrieve configuration values
        const bucket = this.configService.get<string>('APP_NAME');
        const endPoint = this.configService.get<string>('MINIO_ENDPOINT');
        const port = this.configService.get<string>('MINIO_PORT');
        const useSSL = this.configService.get<string>('MINIO_USE_SSL');
        const accessKey = this.configService.get<string>('MINIO_ACCESS_KEY');
        const secretKey = this.configService.get<string>('MINIO_SECRET_KEY');

        // Check for missing environment variables
        const missingVars: string[] = [];
        if (!bucket) missingVars.push('APP_NAME');
        if (!endPoint) missingVars.push('MINIO_ENDPOINT');
        if (!port) missingVars.push('MINIO_PORT');
        if (!accessKey) missingVars.push('MINIO_ACCESS_KEY');
        if (!secretKey) missingVars.push('MINIO_SECRET_KEY');

        if (missingVars.length > 0) {
            this.errors.push(
                `Missing required environment variables: ${missingVars.join(', ')}. Please check the README for configuration details.`,
            );
        } else {
            // Validate bucket name against BucketMinio type
            const validBuckets: BucketMinio[] = ['working-space', 'e-health', 'chat', 'avatar'];
            if (!validBuckets.includes(bucket as BucketMinio)) {
                this.errors.push(
                    `Invalid bucket name: ${bucket}. Must be one of: ${validBuckets.join(', ')}. Please check the README for configuration details.`,
                );
            } else {
                this.bucket = bucket as BucketMinio;
            }

            // Initialize MinIO client if no errors so far
            if (this.errors.length === 0) {
                try {
                    this.minioClient = new Client({
                        endPoint,
                        port: parseInt(port, 10),
                        useSSL: useSSL === 'true',
                        accessKey,
                        secretKey,
                    });
                    // Test connection
                    this.testConnection();
                } catch (error) {
                    this.errors.push(
                        `Failed to initialize MinIO client: ${error.message}. Please check the README for configuration details.`,
                    );
                    this.minioClient = null;
                }
            } else {
                this.minioClient = null;
            }
        }

        this.setupEventListeners();
    }

    // Test MinIO connection
    private async testConnection() {
        if (!this.minioClient) return;
        try {
            await this.minioClient.listBuckets();
            console.log('Successfully connected to MinIO server.');
        } catch (error) {
            this.errors.push(`MinIO connection test failed: ${error.message}. Please check the README for configuration details.`);
            this.minioClient = null;
        }
    }

    private setupEventListeners() {
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
        // Log all errors collected in constructor
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
            } else {
                console.log(`Bucket ${this.bucket} already exists.`);
            }
        } catch (error) {
            console.error(`Failed to initialize bucket ${this.bucket}: ${error.message}. Please check the README for configuration details.`);
        }
    }


    public isConnected(): boolean {
        return !!this.minioClient;
    }

    private getFileExtension(fileName: string): string {
        return fileName.split('.').pop()?.toLowerCase() || '';
    }

    private getDatePrefix(): string {
        const date = new Date();
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        return `${year}-${month}-${day}`;
    }

    private generateFilePath(fileName: string): string {
        const extension = this.getFileExtension(fileName);
        const datePrefix = this.getDatePrefix();
        return `${this.bucket}/${datePrefix}/${extension}/${fileName}`;
    }

    private parsePathMinio(fullPath: string): { bucket: string; path: string } {
        const [bucket, ...rest] = fullPath.split('/');
        const path = rest.join('/');

        return { bucket, path };
    }

    private async ensureBucketExists(bucket: string): Promise<void> {
        const exists = await this.minioClient.bucketExists(bucket);
        if (!exists) {
            await this.minioClient.makeBucket(bucket);
        }
    }

    private validateFileSize(buffer: Buffer | string): void {
        if (typeof buffer === 'string') {
            return;
        }
        if (buffer instanceof Buffer) {
            const fileSize = buffer.length;
            if (fileSize > MAX_FILE_SIZE) {
                throw new Error(`File size ${fileSize} exceeds maximum allowed size of ${MAX_FILE_SIZE} bytes (${MAX_FILE_SIZE / (1024 * 1024)}MB)`);
            }
        }
    }

    private async handleUpload(data: IFile): Promise<string> {
        await this.ensureBucketExists(this.bucket);

        try {
            this.validateFileSize(data.buffer);

            const pathFileUpload = await this.fileQueueService.addUploadJob(
                data,
                this.uploadHandler.bind(this)
            );

            return pathFileUpload;
        } catch (error) {
            this.eventEmitter.emit('upload.failed', {
                taskId: data.fileName,
                error: error.message
            });
            throw error;
        }
    }
    async upload(data: IFile): Promise<string> {
        return this.handleUpload(data);
    }

    private validateImageFile(fileName: string): void {
        const allowedExtensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp'];
        const lowerFileName = fileName.toLowerCase();
        const isValid = allowedExtensions.some(ext => lowerFileName.endsWith(ext));

        if (!isValid) {
            throw new Error('Only image files are allowed for avatars.');
        }
    }

    async uploadAvatar(data: IFile): Promise<string> {
        this.bucket = 'avatar';
        this.validateImageFile(data.fileName);
        return this.handleUpload(data);
    }

    // Handler for actual file upload
    async uploadHandler(data: IFile): Promise<string> {
        const { fileName, mimeType, buffer } = data;
        const metaData = {
            'Content-Type': mimeType,
        };

        const newFilePath = this.generateFilePath(fileName);
        // Lưu path gốc để dùng trong Minio (không bao gồm bucket prefix)
        const minioPath = newFilePath.replace(`${this.bucket}/`, '');
        data.fileName = minioPath;
        try {
            if (buffer.length < MULTIPART_THRESHOLD) {
                // Upload trực tiếp từ buffer nếu file nhỏ
                await this.minioClient.putObject(
                    this.bucket,
                    minioPath,
                    buffer,
                    buffer.length,
                    metaData,
                );
            } else {
                // Sử dụng stream cho file lớn
                const bufferStream = new PassThrough();
                bufferStream.end(buffer);
                await this.minioClient.putObject(
                    this.bucket,
                    minioPath,
                    bufferStream,
                    buffer.length,
                    metaData,
                );
            }
            console.log(`Đã upload ${minioPath} (${buffer.length} bytes)`);
            return newFilePath;
        } catch (error) {
            console.error('Lỗi khi upload lên Minio:', error);
            throw error;
        }
    }

    /**
     * Lấy thông tin file từ Minio
     * @param fileName Tên file trên Minio
     * @returns Metadata của file
     */
    async getFileInfo(fullPath: string): Promise<any> {
        const { bucket, path } = this.parsePathMinio(fullPath)
        try {
            return await this.minioClient.statObject(bucket, path);
        } catch (error) {
            throw new NotFoundException(`Đường dẫn ${fullPath} không tồn tại`);
        }
    }

    /**
     * Tải file từ Minio dưới dạng stream
     * @param fileName Tên file trên Minio
     * @returns Stream của file
     */
    async download(fullPath: string): Promise<NodeJS.ReadableStream> {
        try {
            const { path, bucket } = this.parsePathMinio(fullPath)
            return await this.minioClient.getObject(bucket, path);
        } catch (error) {
            throw new NotFoundException(`Đường dẫn ${fullPath} không tồn tại`);
        }
    }

    /**
       * Lấy nội dung file từ Minio dưới dạng Buffer
       * @param fileName Tên file trên Minio
       * @returns Buffer chứa nội dung file
       * @throws BadRequestException nếu file vượt quá ngưỡng bufferThreshold
       */
    async getBuffer(fullPath: string): Promise<Buffer> {
        try {
            const { path, bucket } = this.parsePathMinio(fullPath);
            // Kiểm tra kích thước file trước khi lấy buffer
            const fileInfo = await this.getFileInfo(path);
            if (fileInfo.size > BUFFER_THRESHOLD) {
                throw new BadRequestException(
                    `File size (${fileInfo.size} bytes) exceeds buffer threshold (${BUFFER_THRESHOLD} bytes)`,
                );
            }

            // Lấy stream của file
            const fileStream = await this.minioClient.getObject(bucket, path);
            const chunks: Buffer[] = [];

            // Chuyển stream thành buffer
            return new Promise((resolve, reject) => {
                fileStream.on('data', (chunk) => chunks.push(chunk));
                fileStream.on('end', () => resolve(Buffer.concat(chunks)));
                fileStream.on('error', (err) => reject(err));
            });
        } catch (error) {
            if (error instanceof NotFoundException || error instanceof BadRequestException) {
                throw error;
            }
            console.error('Lỗi khi lấy buffer từ Minio:', error);
            throw new NotFoundException(`Đường dẫn ${fullPath} không tồn tại`);
        }
    }

    /**
     * Nén buffer bằng GZIP
     * @param buffer Buffer cần nén
     * @returns Buffer đã nén
     */
    private async compressBuffer(buffer: Buffer): Promise<Buffer> {
        try {
            return await this.gzip(buffer);
        } catch (error) {
            console.error('Lỗi khi nén buffer:', error);
            throw new BadRequestException('Không thể nén file');
        }
    }

    /**
     * Giải nén buffer từ GZIP
     * @param compressedBuffer Buffer đã nén
     * @returns Buffer đã giải nén
     */
    private async decompressBuffer(compressedBuffer: Buffer): Promise<Buffer> {
        try {
            return await this.gunzip(compressedBuffer);
        } catch (error) {
            console.error('Lỗi khi giải nén buffer:', error);
            throw new BadRequestException('Không thể giải nén file');
        }
    }

    /**
     * Upload file với nén GZIP
     * @param data IFile data
     * @returns Đường dẫn file đã upload
     */
    async uploadCompressed(data: IFile): Promise<string> {
        try {
            console.log(`Bắt đầu nén file ${data.fileName} (${data.buffer.length} bytes)`);
            
            // Nén buffer
            const compressedBuffer = await this.compressBuffer(data.buffer);
            const compressionRatio = ((data.buffer.length - compressedBuffer.length) / data.buffer.length * 100).toFixed(2);
            
            console.log(`File đã được nén từ ${data.buffer.length} bytes xuống ${compressedBuffer.length} bytes (tiết kiệm ${compressionRatio}%)`);
            
            // Tạo file data mới với buffer đã nén
            const compressedData: IFile = {
                ...data,
                buffer: compressedBuffer,
                fileName: data.fileName + '.gz', // Thêm extension .gz
                mimeType: 'application/gzip'
            };
            
            return await this.handleUpload(compressedData);
        } catch (error) {
            console.error('Lỗi khi upload file đã nén:', error);
            throw error;
        }
    }

    /**
     * Upload file với nén DEFLATE (nhẹ hơn GZIP)
     * @param data IFile data
     * @returns Đường dẫn file đã upload
     */
    async uploadDeflateCompressed(data: IFile): Promise<string> {
        try {
            console.log(`Bắt đầu nén file ${data.fileName} bằng DEFLATE (${data.buffer.length} bytes)`);
            
            // Nén buffer bằng deflate
            const compressedBuffer = await this.deflate(data.buffer);
            const compressionRatio = ((data.buffer.length - compressedBuffer.length) / data.buffer.length * 100).toFixed(2);
            
            console.log(`File đã được nén từ ${data.buffer.length} bytes xuống ${compressedBuffer.length} bytes (tiết kiệm ${compressionRatio}%)`);
            
            // Tạo file data mới với buffer đã nén
            const compressedData: IFile = {
                ...data,
                buffer: compressedBuffer,
                fileName: data.fileName + '.deflate', // Thêm extension .deflate
                mimeType: 'application/deflate'
            };
            
            return await this.handleUpload(compressedData);
        } catch (error) {
            console.error('Lỗi khi upload file đã nén bằng DEFLATE:', error);
            throw error;
        }
    }

    /**
     * Download và giải nén file GZIP từ Minio
     * @param fullPath Đường dẫn đầy đủ của file
     * @returns Buffer đã giải nén
     */
    async downloadAndDecompress(fullPath: string): Promise<Buffer> {
        try {
            console.log(`Bắt đầu download và giải nén file: ${fullPath}`);
            
            // Lấy buffer file đã nén
            const compressedBuffer = await this.getBuffer(fullPath);
            console.log(`Đã download file nén (${compressedBuffer.length} bytes)`);
            
            // Giải nén buffer
            const decompressedBuffer = await this.decompressBuffer(compressedBuffer);
            console.log(`File đã được giải nén thành ${decompressedBuffer.length} bytes`);
            
            return decompressedBuffer;
        } catch (error) {
            console.error('Lỗi khi download và giải nén file:', error);
            throw error;
        }
    }

    /**
     * Download và giải nén file DEFLATE từ Minio
     * @param fullPath Đường dẫn đầy đủ của file
     * @returns Buffer đã giải nén
     */
    async downloadAndInflate(fullPath: string): Promise<Buffer> {
        try {
            console.log(`Bắt đầu download và giải nén file DEFLATE: ${fullPath}`);
            
            // Lấy buffer file đã nén
            const compressedBuffer = await this.getBuffer(fullPath);
            console.log(`Đã download file nén DEFLATE (${compressedBuffer.length} bytes)`);
            
            // Giải nén buffer bằng inflate
            const decompressedBuffer = await this.inflate(compressedBuffer);
            console.log(`File đã được giải nén thành ${decompressedBuffer.length} bytes`);
            
            return decompressedBuffer;
        } catch (error) {
            console.error('Lỗi khi download và giải nén file DEFLATE:', error);
            throw error;
        }
    }

    /**
     * Download file nén và trả về stream đã giải nén
     * @param fullPath Đường dẫn đầy đủ của file
     * @returns Stream đã giải nén
     */
    async downloadCompressedStream(fullPath: string): Promise<NodeJS.ReadableStream> {
        try {
            console.log(`Bắt đầu download stream và giải nén: ${fullPath}`);
            
            // Lấy stream file từ Minio
            const compressedStream = await this.download(fullPath);
            
            // Tạo gunzip stream để giải nén
            const gunzipStream = zlib.createGunzip();
            
            // Pipe compressed stream qua gunzip stream
            return compressedStream.pipe(gunzipStream);
        } catch (error) {
            console.error('Lỗi khi download compressed stream:', error);
            throw error;
        }
    }

    /**
     * Kiểm tra xem file có được nén hay không dựa trên extension
     * @param fullPath Đường dẫn file
     * @returns boolean
     */
    private isCompressedFile(fullPath: string): boolean {
        const lowerPath = fullPath.toLowerCase();
        return lowerPath.endsWith('.gz') || lowerPath.endsWith('.deflate') || lowerPath.endsWith('.gzip');
    }

    /**
     * Download file thông minh - tự động detect và giải nén nếu cần
     * @param fullPath Đường dẫn đầy đủ của file
     * @returns Buffer (đã giải nén nếu file được nén)
     */
    async smartDownload(fullPath: string): Promise<Buffer> {
        try {
            if (this.isCompressedFile(fullPath)) {
                console.log(`Phát hiện file nén, tiến hành giải nén: ${fullPath}`);
                
                if (fullPath.toLowerCase().endsWith('.deflate')) {
                    return await this.downloadAndInflate(fullPath);
                } else {
                    // Default to gzip decompression for .gz and .gzip files
                    return await this.downloadAndDecompress(fullPath);
                }
            } else {
                console.log(`File không nén, download bình thường: ${fullPath}`);
                return await this.getBuffer(fullPath);
            }
        } catch (error) {
            console.error('Lỗi khi smart download:', error);
            throw error;
        }
    }

    /**
     * Upload file với auto-compression (tự động nén nếu file lớn hơn threshold)
     * @param data IFile data
     * @param compressionThreshold Ngưỡng kích thước để bật nén (default: 1MB)
     * @returns Đường dẫn file đã upload
     */
    async uploadWithAutoCompression(data: IFile, compressionThreshold: number = 1024 * 1024): Promise<string> {
        try {
            if (data.buffer.length > compressionThreshold) {
                console.log(`File ${data.fileName} lớn hơn ${compressionThreshold} bytes, tiến hành nén...`);
                return await this.uploadCompressed(data);
            } else {
                console.log(`File ${data.fileName} nhỏ hơn ${compressionThreshold} bytes, upload bình thường...`);
                return await this.upload(data);
            }
        } catch (error) {
            console.error('Lỗi khi upload với auto compression:', error);
            throw error;
        }
    }

    /**
     * Tạo presigned URL để upload/download
     * @param {string} objectName - Tên object trong Minio
     * @param {string} method - 'GET' hoặc 'PUT'
     * @param {number} expiry - Thời gian hết hạn (giây)
     * @returns {string} - URL tạm thời
     */
    async generatePresignedUrl(objectName: string, method = 'GET', expiry = 3600) {
        return await this.minioClient.presignedUrl(method, this.bucket, objectName, expiry);
    }
}