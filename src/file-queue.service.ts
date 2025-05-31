// file-queue.service.ts
import { Injectable, OnModuleInit } from '@nestjs/common';
import { IFile } from 'src/interfaces/file.interface';
import { EventEmitter2 } from '@nestjs/event-emitter';
import * as Queue from 'better-queue';

@Injectable()
export class FileQueueService implements OnModuleInit {
    private uploadQueue: Queue;
    private downloadQueue: Queue;

    constructor(private eventEmitter: EventEmitter2) { }

    onModuleInit() {
        // Khởi tạo upload queue với file store để persist jobs
        this.uploadQueue = new Queue(async (task, cb) => {
            try {
                const result = await task.handler(task.data);
                cb(null, result);
            } catch (error) {
                cb(error);
            }
        }, {
            // Số lượng jobs xử lý đồng thời
            concurrent: 3,
            // Số lần retry khi fail
            maxRetries: 3,
            // Thời gian chờ giữa các lần retry
            retryDelay: 5000,
            // Callback xử lý priority
            priority: (task, cb) => {
                cb(null, task.priority || 0);
            },
            // Timeout cho mỗi job
            // timeout: 30  60  1000 // 30 phút
        });

        // Tương tự cho download queue
        this.downloadQueue = new Queue(async (task, cb) => {
            try {
                const result = await task.handler(task.data);
                cb(null, result);
            } catch (error) {
                cb(error);
            }
        }, {
            concurrent: 5,
            maxRetries: 3,
            retryDelay: 5000,
            maxTimeout: 30 * 60 * 1000
        });

        // Xử lý các events của queue
        this.setupQueueEvents();
    }

    private setupQueueEvents() {
        // Xử lý khi job hoàn thành
        this.uploadQueue.on('task_finish', (taskId, result) => {
            this.eventEmitter.emit('upload.completed', { taskId, result });
        });

        // Xử lý khi job thất bại
        this.uploadQueue.on('task_failed', (taskId, error) => {
            this.eventEmitter.emit('upload.failed', { taskId, error });
        });

        // Xử lý khi queue đầy
        this.uploadQueue.on('task_queued', () => {
            console.warn('Upload queue is saturated');
        });

        // Tương tự cho download queue
        this.downloadQueue.on('task_finish', (taskId, result) => {
            this.eventEmitter.emit('download.completed', { taskId, result });
        });

        this.downloadQueue.on('task_failed', (taskId, error) => {
            this.eventEmitter.emit('download.failed', { taskId, error });
        });
    }

    // Thêm job upload vào queue
    addUploadJob(data: IFile, handler: Function, priority = 0): Promise<string> {
        return new Promise((resolve, reject) => {
            const taskId = this.generateTaskId();
            this.uploadQueue.push({
                id: taskId,
                data,
                handler,
                priority
            }, (err, result) => {
                if (err) reject(err);
                else resolve(result);
            });
        });
    }

    // Thêm job download vào queue
    addDownloadJob(fileName: string, handler: Function): Promise<string> {
        return new Promise((resolve, reject) => {
            const taskId = this.generateTaskId();
            this.downloadQueue.push({
                id: taskId,
                data: fileName,
                handler
            }, (err) => {
                if (err) reject(err);
                else resolve(taskId);
            });
        });
    }


    // Hủy job trong queue
    cancelJob(queueType: 'upload' | 'download', taskId: string): Promise<void> {
        return new Promise((resolve, reject) => {
            const queue = queueType === 'upload' ? this.uploadQueue : this.downloadQueue;
            queue.cancel(taskId, (err?: any) => {
                if (err) reject(err);
                else resolve();
            });
        });
    }

    // Tạo unique task ID
    private generateTaskId(): string {
        return `${Date.now()}-${Math.random().toString(36).substring(2, 11)}`;
    }

    // Lấy trạng thái của job
    getJobStats(queueType: 'upload' | 'download'): any {
        const queue = queueType === 'upload' ? this.uploadQueue : this.downloadQueue;
        return queue.getStats();
    }
}