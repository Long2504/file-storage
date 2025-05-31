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
exports.FileStorageService = void 0;
const common_1 = require("@nestjs/common");
const minio_service_1 = require("./minio/minio.service");
let FileStorageService = class FileStorageService {
    constructor(minioStorage) {
        this.minioStorage = minioStorage;
    }
    getProvider(providerName) {
        switch (providerName) {
            case 'minio':
                return this.minioStorage;
            default:
                console.error(`Invalid file storage provider configured: ${providerName}`);
                return null;
        }
    }
    async upload(fileUpload) {
        const { data, provider } = fileUpload;
        this.currentProvider = this.getProvider(provider.type);
        return this.currentProvider.upload(data, provider);
    }
    async uploadAvatar(fileUpload) {
        const { data, provider } = fileUpload;
        this.currentProvider = this.getProvider(provider.type);
        return this.currentProvider.uploadAvatar(data, provider);
    }
    async getBufferFile(input) {
        const { fileName, provider } = input;
        this.currentProvider = this.getProvider(provider.type);
        return this.currentProvider.getBuffer(fileName, provider);
    }
    async download(fileName, provider) {
        this.currentProvider = this.getProvider(provider);
        return this.currentProvider.download(fileName);
    }
    async getFileInfo(fileName, provider) {
        this.currentProvider = this.getProvider(provider);
        return this.currentProvider.getFileInfo(fileName);
    }
};
exports.FileStorageService = FileStorageService;
exports.FileStorageService = FileStorageService = __decorate([
    (0, common_1.Injectable)(),
    __metadata("design:paramtypes", [minio_service_1.MinioStorageService])
], FileStorageService);
//# sourceMappingURL=file-storage.service.js.map