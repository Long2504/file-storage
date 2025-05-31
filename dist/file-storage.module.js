"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.FileStorageModule = void 0;
const common_1 = require("@nestjs/common");
const config_1 = require("@nestjs/config");
const file_queue_service_1 = require("./file-queue.service");
const event_emitter_1 = require("@nestjs/event-emitter");
const file_storage_service_1 = require("./file-storage.service");
const minio_service_1 = require("./minio/minio.service");
let FileStorageModule = class FileStorageModule {
};
exports.FileStorageModule = FileStorageModule;
exports.FileStorageModule = FileStorageModule = __decorate([
    (0, common_1.Module)({
        imports: [config_1.ConfigModule, event_emitter_1.EventEmitterModule.forRoot()],
        providers: [file_storage_service_1.FileStorageService, minio_service_1.MinioStorageService, file_queue_service_1.FileQueueService],
        exports: [file_storage_service_1.FileStorageService],
    })
], FileStorageModule);
//# sourceMappingURL=file-storage.module.js.map