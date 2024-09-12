package io.omniledger.filerelay;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class FileServiceTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileServiceTest.class);

    @RegisterExtension
    static FileServiceExtension testContext = new FileServiceExtension();

    @Test
    public void testCreate() throws Exception {
        Path basePath = testContext.basePath();

        CompletableFuture<Path> createFuture = testContext.onFileCreate();

        // create file named "foo"
        File file = new File(basePath.toFile(), "foo");
        boolean created = file.createNewFile();
        assert created;

        // wait at most a second for callback to be triggered
        Path fsCreated = createFuture.get(1, TimeUnit.SECONDS);

        // these two files should be the same
        Assertions.assertEquals(file, fsCreated.toFile());
    }

    @Test
    public void testModify() throws Exception {
        Path basePath = testContext.basePath();

        File modifiedFile = new File(basePath.toFile(), "foo");
        boolean created = modifiedFile.createNewFile();
        assert created;

        CompletableFuture<Path> appendFuture = testContext.onFileAppend(modifiedFile);
        Assertions.assertFalse(appendFuture.isDone());

        // append stuff into file
        char[] chars = "foobar".toCharArray();
        FileWriter fileWriter = new FileWriter(modifiedFile);

        // future can't be finished until file is appended
        Assertions.assertFalse(appendFuture.isDone());

        fileWriter.write(chars);
        fileWriter.flush();
        fileWriter.close();

        // if it doesn't finish in at most a second, we have a problem
        appendFuture.get(1, TimeUnit.SECONDS);

        char[] buffer = new char[chars.length];
        FileReader fileReader = new FileReader(modifiedFile);
        int read = fileReader.read(buffer);

        /*
         this is strictly not necessarily true according to spec, so if starts failing
         on some weird file-system, just append the buffer-array until we have all chars
         */
        Assertions.assertEquals(read, buffer.length);
        Assertions.assertArrayEquals(chars, buffer);
    }
}