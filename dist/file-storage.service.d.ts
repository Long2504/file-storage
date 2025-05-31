import { MinioStorageService } from "./minio/minio.service";
import { IFileUpload, IGetBufferFile } from "./interfaces/file.interface";
export declare class FileStorageService {
    private minioStorage;
    private currentProvider;
    constructor(minioStorage: MinioStorageService);
    private getProvider;
    upload(fileUpload: IFileUpload): Promise<string>;
    uploadAvatar(fileUpload: IFileUpload): Promise<string>;
    getBufferFile(input: IGetBufferFile): Promise<Buffer>;
    download(fileName: string, provider: string): Promise<Buffer>;
    getFileInfo(fileName: string, provider: string): Promise<any>;
}
