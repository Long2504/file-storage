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
exports.FileQueueService = void 0;
const common_1 = require("@nestjs/common");
const event_emitter_1 = require("@nestjs/event-emitter");
const Queue = require("better-queue");
let FileQueueService = class FileQueueService {
    constructor(eventEmitter) {
        this.eventEmitter = eventEmitter;
    }
    onModuleInit() {
        this.uploadQueue = new Queue(async (task, cb) => {
            try {
                const result = await task.handler(task.data);
                cb(null, result);
            }
            catch (error) {
                cb(error);
            }
        }, {
            concurrent: 3,
            maxRetries: 3,
            retryDelay: 5000,
            priority: (task, cb) => {
                cb(null, task.priority || 0);
            },
        });
        this.downloadQueue = new Queue(async (task, cb) => {
            try {
                const result = await task.handler(task.data);
                cb(null, result);
            }
            catch (error) {
                cb(error);
            }
        }, {
            concurrent: 5,
            maxRetries: 3,
            retryDelay: 5000,
            maxTimeout: 30 * 60 * 1000
        });
        this.setupQueueEvents();
    }
    setupQueueEvents() {
        this.uploadQueue.on('task_finish', (taskId, result) => {
            this.eventEmitter.emit('upload.completed', { taskId, result });
        });
        this.uploadQueue.on('task_failed', (taskId, error) => {
            this.eventEmitter.emit('upload.failed', { taskId, error });
        });
        this.uploadQueue.on('task_queued', () => {
            console.warn('Upload queue is saturated');
        });
        this.downloadQueue.on('task_finish', (taskId, result) => {
            this.eventEmitter.emit('download.completed', { taskId, result });
        });
        this.downloadQueue.on('task_failed', (taskId, error) => {
            this.eventEmitter.emit('download.failed', { taskId, error });
        });
    }
    addUploadJob(data, handler, priority = 0) {
        return new Promise((resolve, reject) => {
            const taskId = this.generateTaskId();
            this.uploadQueue.push({
                id: taskId,
                data,
                handler,
                priority
            }, (err, result) => {
                if (err)
                    reject(err);
                else
                    resolve(result);
            });
        });
    }
    addDownloadJob(fileName, handler) {
        return new Promise((resolve, reject) => {
            const taskId = this.generateTaskId();
            this.downloadQueue.push({
                id: taskId,
                data: fileName,
                handler
            }, (err) => {
                if (err)
                    reject(err);
                else
                    resolve(taskId);
            });
        });
    }
    cancelJob(queueType, taskId) {
        return new Promise((resolve, reject) => {
            const queue = queueType === 'upload' ? this.uploadQueue : this.downloadQueue;
            queue.cancel(taskId, (err) => {
                if (err)
                    reject(err);
                else
                    resolve();
            });
        });
    }
    generateTaskId() {
        return `${Date.now()}-${Math.random().toString(36).substring(2, 11)}`;
    }
    getJobStats(queueType) {
        const queue = queueType === 'upload' ? this.uploadQueue : this.downloadQueue;
        return queue.getStats();
    }
};
exports.FileQueueService = FileQueueService;
exports.FileQueueService = FileQueueService = __decorate([
    (0, common_1.Injectable)(),
    __metadata("design:paramtypes", [event_emitter_1.EventEmitter2])
], FileQueueService);
//# sourceMappingURL=file-queue.service.js.map