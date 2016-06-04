
package com.cds.learn.common.alluxio;

import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AlluxioUtils {

    private static final Logger LOG = LoggerFactory.getLogger(AlluxioUtils.class);

    //如下都是日志没什么意思
    static public boolean outputEnable = true;

    static public void enableOutput() {
        outputEnable = true;
    }

    static public void disableOutput() {
        outputEnable = false;
    }

    static public void log(String str) {
        if (outputEnable) {
            System.out.print("  --- " + str + "\n\n");
        }
    }

    static public int utf8LenCounter(CharSequence sequence) {
        int count = 0;
        for (int i = 0, len = sequence.length(); i < len; i++) {
            char ch = sequence.charAt(i);
            if (ch <= 0x7F) {
                count++;
            } else if (ch <= 0x7FF) {
                count += 2;
            } else if (Character.isHighSurrogate(ch)) {
                count += 4;
                ++i;
            } else {
                count += 3;
            }
        }
        return count;
    }

    public static void handleAlluxioException(Exception exception) {
        if (exception instanceof FileAlreadyExistsException) {
            LOG.error("get a FileAlreadyExistException", exception);
        }
        if (exception instanceof InvalidPathException) {
            LOG.error("the alluxio path is invalid!", exception);
        }
        if (exception instanceof IOException) {
            LOG.error("get a IOException", exception);
        }
        if (exception instanceof AlluxioException) {
            LOG.error("get a alluxio exception!", exception);
        }
        if (exception instanceof DirectoryNotEmptyException) {
            LOG.error("the directory is not empty!", exception);
        }
        if (exception instanceof FileDoesNotExistException) {
            LOG.error("the file does not exist!", exception);
        }
    }


    static public class TimeMeasure {
        private long m_startTime = 0;
        private long m_endTime = 0;
        private long m_elapsedTime = 0;

        public TimeMeasure() {
        }

        public void start() {
            m_startTime = System.currentTimeMillis();
        }

        public void pause() {
            m_endTime = System.currentTimeMillis();
        }

        public void cont() {
            m_elapsedTime += m_endTime - m_startTime;
            m_startTime = System.currentTimeMillis();
        }

        public void reset() {
            m_startTime = m_endTime = m_elapsedTime = 0;
        }

        public long getElapsedTime() {
            m_elapsedTime += m_endTime - m_startTime;
            return m_elapsedTime;
        }
    }
}
