import { Module } from "@nestjs/common";
import { ConfigModule } from "@nestjs/config";
import { FileQueueService } from "./file-queue.service";
import { EventEmitterModule } from "@nestjs/event-emitter";
import { FileStorageService } from "./file-storage.service";
import { MinioStorageService } from "./minio/minio.service";

@Module({
    imports: [ConfigModule, EventEmitterModule.forRoot()],
    providers: [FileStorageService, MinioStorageService, FileQueueService],
    exports: [FileStorageService],
})

export class FileStorageModule { }
