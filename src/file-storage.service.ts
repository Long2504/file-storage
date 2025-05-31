import { Injectable } from "@nestjs/common";
import { MinioStorageService } from "./minio/minio.service";
import { IFileUpload, IGetBufferFile } from "./interfaces/file.interface";


@Injectable()
export class FileStorageService {
  private currentProvider: any;

  constructor(
    private minioStorage: MinioStorageService,
  ) { }


  private getProvider(providerName: string): any {
    switch (providerName) {
      case 'minio':
        return this.minioStorage
      default:
        console.error(`Invalid file storage provider configured: ${providerName}`);
        return null;
    }
  }


  public async upload(fileUpload: IFileUpload): Promise<string> {
    const { data, provider } = fileUpload;
    this.currentProvider = this.getProvider(provider.type);
    return this.currentProvider.upload(data, provider);
  }

  public async uploadAvatar(fileUpload: IFileUpload): Promise<string> {
    const { data, provider } = fileUpload;
    this.currentProvider = this.getProvider(provider.type);
    return this.currentProvider.uploadAvatar(data, provider);
  }

  public async getBufferFile(input: IGetBufferFile): Promise<Buffer> {
    const { fileName, provider } = input
    this.currentProvider = this.getProvider(provider.type);
    return this.currentProvider.getBuffer(fileName, provider);
  }

  public async download(fileName: string, provider: string): Promise<Buffer> {
    this.currentProvider = this.getProvider(provider);
    return this.currentProvider.download(fileName);
  }

  public async getFileInfo(fileName: string, provider: string): Promise<any> {
    this.currentProvider = this.getProvider(provider);
    return this.currentProvider.getFileInfo(fileName);
  }
}