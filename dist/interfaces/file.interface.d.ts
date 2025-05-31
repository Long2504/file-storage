export interface IFile {
    fileName: string;
    buffer: Buffer;
    mimeType: string;
}
export interface IFileUpload {
    data: IFile;
    provider: Provider;
}
export interface IGetBufferFile {
    fileName: string;
    provider: Provider;
}
interface BaseProvider {
    type: string;
}
export type BucketMinio = 'working-space' | 'e-health' | 'chat' | 'avatar';
export interface MinioProvider extends BaseProvider {
    type: 'minio';
    option?: {};
}
interface OtherProvider extends BaseProvider {
    type: 'other';
}
type Provider = MinioProvider | OtherProvider;
export {};
