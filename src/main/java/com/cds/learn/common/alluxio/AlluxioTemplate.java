package com.cds.learn.common.alluxio;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.ClientContext;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.ExistsOptions;
import alluxio.client.file.options.FreeOptions;
import alluxio.client.file.options.GetStatusOptions;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.client.file.options.LoadMetadataOptions;
import alluxio.client.file.options.MountOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.options.RenameOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.client.file.options.UnmountOptions;
import alluxio.client.file.policy.MostAvailableFirstPolicy;
import alluxio.client.file.policy.SpecificHostPolicy;
import alluxio.exception.AlluxioException;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

/**
 * 命名规则采用官方写法，也就是说如果是成员变量，那么在变量名前加“m”。
 */
@SuppressWarnings("unused") public class AlluxioTemplate {

    private static final Logger LOG = LoggerFactory.getLogger(AlluxioTemplate.class);

    private final FileSystem mFileSystem;

    /**
     * use this constructor if you use all default alluxio-site configuration
     */
    public AlluxioTemplate() {
        mFileSystem = FileSystem.Factory.get();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Connecting to Master..." + ClientContext.getMasterAddress());
        }
    }

    /**
     * 用自定义的路径来初始化
     *
     * @param masterLocation 比如alluxio://localhost:19999
     */
    public AlluxioTemplate(String masterLocation) {
        ClientContext.getConf()
            .set(Constants.MASTER_HOSTNAME, new AlluxioURI(masterLocation).getHost());
        ClientContext.init();
        mFileSystem = FileSystem.Factory.get();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Connecting to Master..." + ClientContext.getMasterAddress());
        }
    }

    public <T> T execute(AlluxioCallback<T> action) throws Exception {
        Preconditions.checkNotNull(action, "the alluxio action should not be null!");
        try {
            return action.doInAlluxio(mFileSystem);
        } catch (Exception e) {
            AlluxioUtils.handleAlluxioException(e);
            throw e;
        }
    }

    public void executeNoResult(AlluxioNoResult action) throws Exception {
        try {
            action.doInAlluxio(mFileSystem);
        } catch (Exception e) {
            AlluxioUtils.handleAlluxioException(e);
            throw e;
        }
    }


    /**
     * @see alluxio.client.file.FileSystem#createFile(AlluxioURI)
     */
    public FileOutStream createFile(String path) throws Exception {
        return execute(fileSystem -> fileSystem.createFile(new AlluxioURI(path)));
    }

    /**
     * 递归的创建alluxio文件
     *
     * @param path alluxio 路劲
     * @throws Exception
     */
    public FileOutStream createFileRecursive(String path) throws Exception {
        CreateFileOptions options = CreateFileOptions.defaults();
        options.setRecursive(true);
        return execute(fileSystem -> fileSystem.createFile(new AlluxioURI(path), options));
    }

    /**
     * 递归的创建alluxio文件,并且设置自定义的块大小，单位是MB。
     *
     * @param path alluxio 路径
     * @throws Exception
     */
    public FileOutStream createFileRecursive(String path, long size) throws Exception {
        CreateFileOptions options = CreateFileOptions.defaults();
        options.setRecursive(true);
        options.setBlockSizeBytes(1024 * 1024 * size);
        return execute(fileSystem -> fileSystem.createFile(new AlluxioURI(path), options));
    }

    /**
     * @see alluxio.client.file.FileSystem#createFile(AlluxioURI, CreateFileOptions)
     */
    public FileOutStream createFile(String path, CreateFileOptions options) throws Exception {
        return execute(fileSystem -> fileSystem.createFile(new AlluxioURI(path), options));
    }

    private FileOutStream getFileOutStream(AlluxioURI uri, WriteType type, String targetWorker)
        throws AlluxioException, IOException {

        CreateFileOptions writeOptions = CreateFileOptions.defaults().setWriteType(type);

        if (targetWorker.equals(AlluxioConsts.MOST_AVAILABLE_FIRST)) {
            writeOptions.setLocationPolicy(new MostAvailableFirstPolicy());
        } else if (!targetWorker.equals(AlluxioConsts.NON_SPECIFIED_WORKER)) {
            writeOptions.setLocationPolicy(new SpecificHostPolicy(targetWorker));
        }

        return mFileSystem.createFile(uri, writeOptions);
    }

    private void writeFile(AlluxioURI uri, String msg, WriteType type, String targetWorker)
        throws IOException, AlluxioException {

        //        if (LOG.isDebugEnabled()) {
        LOG.debug("Writing data \"" + msg + "\" to " + uri.getPath() + " at " + targetWorker + (type
            .isCache() ? " memory" : "") + (type.isThrough() ? " disk" : ""));
        //        }

        AlluxioUtils.TimeMeasure tm = new AlluxioUtils.TimeMeasure();
        tm.start();

        ByteBuffer buf = ByteBuffer.wrap(msg.getBytes(Charset.forName("UTF-8")));
        //因为FileOutStream实现了AutoCloseable接口，所以使用try with resource语法，
        // 减少了finally来关闭资源，称之为Automatic Resource Management(自动资源管理)。
        try (FileOutStream os = getFileOutStream(uri, type, targetWorker)) {
            os.write(buf.array());
        } catch (Exception e) {
            throw new IOException(e);
        }
        tm.pause();
        if (LOG.isDebugEnabled()) {
            LOG.debug("[write File] ElapsedTime = " + tm.getElapsedTime() + " ms!");
        }
    }

    private void writeLargeFile(AlluxioURI uri, String msg, double sizeMB, WriteType type,
        String targetWorker) throws IOException, AlluxioException {


        AlluxioUtils.TimeMeasure tm = new AlluxioUtils.TimeMeasure();
        tm.start();

        int remainSizeByte = (int) sizeMB * 1024 * 1024;
        int len = AlluxioUtils.utf8LenCounter(msg);

        if (remainSizeByte == 0 || len == 0) {
            LOG.error(
                "[the len is 0 or the remain size byte is 0]: " + "the the remain size byte is"
                    + remainSizeByte + " the len is :" + len);
            return;
        }

        ByteBuffer buf = ByteBuffer.wrap(msg.getBytes(Charset.forName("UTF-8")));
        try (FileOutStream os = getFileOutStream(uri, type, targetWorker)) {
            while ((remainSizeByte -= len) > 0) {
                os.write(buf.array());
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
        tm.pause();

        if (LOG.isDebugEnabled()) {
            LOG.debug("[write large File] ElapsedTime = " + tm.getElapsedTime() + " ms!");
        }
    }

    private void writeLargeFileLocal(String filepath, String msg, double sizeMB)
        throws IOException, AlluxioException {
        AlluxioUtils.TimeMeasure tm = new AlluxioUtils.TimeMeasure();
        tm.start();

        int remainSizeByte = (int) sizeMB * 1024 * 1024;
        int len = AlluxioUtils.utf8LenCounter(msg);

        if (remainSizeByte == 0 || len == 0) {
            LOG.error("Zero length input!");
            return;
        }
        ByteBuffer buf = ByteBuffer.wrap(msg.getBytes(Charset.forName("UTF-8")));
        FileOutputStream os = new FileOutputStream(filepath);
        try {
            while ((remainSizeByte -= len) > 0) {
                os.write(buf.array());
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e);
        } finally {
            os.flush();
            os.getFD().sync();
            os.close();
        }
        tm.pause();
        if (LOG.isDebugEnabled()) {
            LOG.debug("[write large File to local] ElapsedTime = " + tm.getElapsedTime() + " ms!");
        }
    }

    private void readFile(AlluxioURI uri, ReadType type, boolean toPrint)
        throws IOException, AlluxioException {
        readFile(uri, type, toPrint, AlluxioConsts.NON_SPECIFIED_WORKER);
    }

    private void readFile(AlluxioURI uri, ReadType type, boolean toPrint, String cacheLocation)
        throws AlluxioException, IOException {

        AlluxioUtils.TimeMeasure tm = new AlluxioUtils.TimeMeasure();
        tm.start();

        OpenFileOptions readOptions = OpenFileOptions.defaults().setReadType(type);
        if (!cacheLocation.equals(AlluxioConsts.NON_SPECIFIED_WORKER) && !cacheLocation
            .equals(AlluxioConsts.MOST_AVAILABLE_FIRST)) {
            readOptions.setLocationPolicy(new SpecificHostPolicy(cacheLocation));
        }


        //ByteBuffer buf = ByteBuffer.allocate((int) is.remaining());
        //noinspection ResultOfMethodCallIgnored
        try (FileInStream is = mFileSystem.openFile(uri, readOptions)) {
            byte[] bytes = new byte[10000000];
            int bytesRead;
            while ((bytesRead = is.read(bytes)) != -1) {
                String msg = new String(bytes, 0, bytesRead, Charset.forName("UTF-8"));
                if (toPrint) {
                    LOG.info("");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e);
        }

        tm.pause();
    }

    /**
     * @param path alluxio path
     * @throws Exception
     * @see alluxio.client.file.FileSystem#createDirectory(AlluxioURI)
     */
    public void createDirectory(String path) throws Exception {
        executeNoResult(fileSystem -> fileSystem.createDirectory(new AlluxioURI(path)));
    }

    /**
     * @param path    alluxio path
     * @param options options to associate with this operation
     * @throws Exception
     * @see alluxio.client.file.FileSystem#createDirectory(AlluxioURI, CreateDirectoryOptions)
     */
    public void createDirectory(String path, CreateDirectoryOptions options) throws Exception {
        executeNoResult(fileSystem -> fileSystem.createDirectory(new AlluxioURI(path), options));
    }

    /**
     * 递归的创建alluxio的文件夹
     *
     * @param path alluxio路径
     * @throws Exception
     */
    public void createDirRecursive(String path) throws Exception {
        CreateDirectoryOptions options = CreateDirectoryOptions.defaults();
        options.setRecursive(true);
        executeNoResult(fileSystem -> fileSystem.createDirectory(new AlluxioURI(path), options));
    }

    /**
     * @param path the path to delete in Alluxio space
     * @throws Exception
     * @see alluxio.client.file.FileSystem#delete(AlluxioURI)
     */
    public void delete(String path) throws Exception {
        executeNoResult(fileSystem -> fileSystem.delete(new AlluxioURI(path)));
    }

    /**
     * @param path the path to delete in Alluxio space
     * @throws Exception
     * @see alluxio.client.file.FileSystem#delete(AlluxioURI, DeleteOptions)
     */
    public void delete(String path, DeleteOptions options) throws Exception {
        executeNoResult(fileSystem -> fileSystem.delete(new AlluxioURI(path), options));
    }

    /**
     * 递归删除Alluxio上的文件或者目录
     *
     * @param path the path to delete in Alluxio space
     * @throws Exception
     * @see alluxio.client.file.FileSystem#delete(AlluxioURI, DeleteOptions)
     */
    public void deleteRecursive(String path) throws Exception {
        DeleteOptions options = DeleteOptions.defaults();
        options.setRecursive(true);
        executeNoResult(fileSystem -> fileSystem.delete(new AlluxioURI(path), options));
    }


    /**
     * @see alluxio.client.file.FileSystem#exists(AlluxioURI)
     */
    public boolean exists(String path) throws Exception {
        return execute(fileSystem -> fileSystem.exists(new AlluxioURI(path)));
    }

    /**
     * @see alluxio.client.file.FileSystem#exists(AlluxioURI, ExistsOptions)
     */
    public boolean exists(String path, ExistsOptions options) throws Exception {
        return execute(fileSystem -> fileSystem.exists(new AlluxioURI(path), options));
    }

    /**
     * @see alluxio.client.file.FileSystem#free(AlluxioURI)
     */
    public void free(String path) throws Exception {
        executeNoResult(fileSystem -> fileSystem.free(new AlluxioURI(path)));
    }

    /**
     * @see alluxio.client.file.FileSystem#free(AlluxioURI, FreeOptions)
     */
    public void free(String path, FreeOptions options) throws Exception {
        executeNoResult(fileSystem -> fileSystem.free(new AlluxioURI(path), options));
    }

    /**
     * 递归的free Alluxio上的空间
     *
     * @see alluxio.client.file.FileSystem#free(AlluxioURI, FreeOptions)
     */
    public void freeRecursive(String path) throws Exception {
        FreeOptions options = FreeOptions.defaults();
        options.setRecursive(true);
        executeNoResult(fileSystem -> fileSystem.free(new AlluxioURI(path), options));
    }


    /**
     * @see alluxio.client.file.FileSystem#getStatus(AlluxioURI)
     */
    public URIStatus getStatus(String path) throws Exception {
        return execute(fileSystem -> fileSystem.getStatus(new AlluxioURI(path)));
    }

    /**
     * @see alluxio.client.file.FileSystem#getStatus(AlluxioURI, GetStatusOptions)
     */
    public URIStatus getStatus(String path, GetStatusOptions options) throws Exception {
        return execute(fileSystem -> fileSystem.getStatus(new AlluxioURI(path), options));
    }

    /**
     * @see alluxio.client.file.FileSystem#listStatus(AlluxioURI)
     */
    public List<URIStatus> listStatus(String path) throws Exception {
        return execute(fileSystem -> fileSystem.listStatus(new AlluxioURI(path)));
    }


    /**
     * @see alluxio.client.file.FileSystem#listStatus(AlluxioURI, ListStatusOptions)
     */
    public List<URIStatus> listStatus(String path, ListStatusOptions options) throws Exception {
        return execute(fileSystem -> fileSystem.listStatus(new AlluxioURI(path), options));
    }


    /**
     * @see alluxio.client.file.FileSystem#loadMetadata(AlluxioURI)
     */
    public void loadMetadata(String path) throws Exception {
        executeNoResult(fileSystem -> fileSystem.loadMetadata(new AlluxioURI(path)));
    }

    /**
     * @see alluxio.client.file.FileSystem#loadMetadata(AlluxioURI, LoadMetadataOptions)
     */
    public void loadMetadata(String path, LoadMetadataOptions options) throws Exception {
        executeNoResult(fileSystem -> fileSystem.loadMetadata(new AlluxioURI(path), options));
    }

    /**
     * 递归的加载metadata
     *
     * @see alluxio.client.file.FileSystem#loadMetadata(AlluxioURI, LoadMetadataOptions)
     */
    public void loadMetadataRecursive(String path) throws Exception {
        LoadMetadataOptions options = LoadMetadataOptions.defaults();
        options.setRecursive(true);
        executeNoResult(fileSystem -> fileSystem.loadMetadata(new AlluxioURI(path), options));
    }

    /**
     * @see alluxio.client.file.FileSystem#mount(AlluxioURI, AlluxioURI)
     */
    public void mount(String alluxioPath, String ufsPath) throws Exception {
        executeNoResult(
            fileSystem -> fileSystem.mount(new AlluxioURI(alluxioPath), new AlluxioURI(ufsPath)));
    }

    /**
     * @see alluxio.client.file.FileSystem#mount(AlluxioURI, AlluxioURI, MountOptions)
     */
    public void mount(String alluxioPath, String ufsPath, MountOptions options) throws Exception {
        executeNoResult(fileSystem -> fileSystem
            .mount(new AlluxioURI(alluxioPath), new AlluxioURI(ufsPath), options));
    }

    /**
     * @see alluxio.client.file.FileSystem#openFile(AlluxioURI)
     */
    public FileInStream openFile(String path) throws Exception {
        return execute(fileSystem -> fileSystem.openFile(new AlluxioURI(path)));
    }

    /**
     * @see alluxio.client.file.FileSystem#openFile(AlluxioURI, OpenFileOptions)
     */
    public FileInStream openFile(String path, OpenFileOptions options) throws Exception {
        return execute(fileSystem -> fileSystem.openFile(new AlluxioURI(path), options));
    }


    /**
     * @see alluxio.client.file.FileSystem#rename(AlluxioURI, AlluxioURI)
     */
    public void rename(String src, String dst) throws Exception {
        executeNoResult(fileSystem -> fileSystem.rename(new AlluxioURI(src), new AlluxioURI(dst)));
    }

    /**
     * @see alluxio.client.file.FileSystem#rename(AlluxioURI, AlluxioURI, RenameOptions)
     */
    public void rename(String src, String dst, RenameOptions options) throws Exception {
        executeNoResult(
            fileSystem -> fileSystem.rename(new AlluxioURI(src), new AlluxioURI(dst), options));
    }


    /**
     * @see alluxio.client.file.FileSystem#setAttribute(AlluxioURI)
     */
    public void setAttribute(String path) throws Exception {
        executeNoResult(fileSystem -> fileSystem.setAttribute(new AlluxioURI(path)));
    }

    /**
     * @see alluxio.client.file.FileSystem#setAttribute(AlluxioURI, SetAttributeOptions)
     */
    // FIXME: 2016/5/12 这里持久化属性不可用，原因未知！！！！！！
    public void setAttribute(String path, SetAttributeOptions options) throws Exception {
        executeNoResult(fileSystem -> fileSystem.setAttribute(new AlluxioURI(path), options));
    }

    /**
     * @see alluxio.client.file.FileSystem#unmount(AlluxioURI)
     */
    public void unmount(String path) throws Exception {
        executeNoResult(fileSystem -> fileSystem.unmount(new AlluxioURI(path)));
    }

    /**
     * @see alluxio.client.file.FileSystem#unmount(AlluxioURI, UnmountOptions)
     */
    public void unmount(String path, UnmountOptions options) throws Exception {
        executeNoResult(fileSystem -> fileSystem.unmount(new AlluxioURI(path), options));
    }

    public static void main(String[] args) throws Exception {
        AlluxioTemplate template = new AlluxioTemplate("alluxio://207.207.77.60:19999");
        System.out.println(template.listStatus("/anancds/cds1/").get(0).getName());
        template.rename("/anancds/cds1", "/anancds/cds1");
        template.free("/mm");
    }

}
