import { OnModuleInit } from '@nestjs/common';
import { IFile } from 'src/interfaces/file.interface';
import { EventEmitter2 } from '@nestjs/event-emitter';
export declare class FileQueueService implements OnModuleInit {
    private eventEmitter;
    private uploadQueue;
    private downloadQueue;
    constructor(eventEmitter: EventEmitter2);
    onModuleInit(): void;
    private setupQueueEvents;
    addUploadJob(data: IFile, handler: Function, priority?: number): Promise<string>;
    addDownloadJob(fileName: string, handler: Function): Promise<string>;
    cancelJob(queueType: 'upload' | 'download', taskId: string): Promise<void>;
    private generateTaskId;
    getJobStats(queueType: 'upload' | 'download'): any;
}
