package io.omniledger.filerelay;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class FileServiceExtension implements BeforeEachCallback, AfterEachCallback, AfterAllCallback {

	private static final String TEST_PATH =
			System.getProperty("java.io.tmpdir") + File.separatorChar +
					"o8r_fs_tests" + File.separatorChar;

	private FileService fileService;
	private CompletableFuture<Void> testFuture;
	private Path basePath;

	/**
	 * Recreate entire environment before each test.
	 * */
	@Override
	public void beforeEach(ExtensionContext extensionContext) throws Exception {
		basePath = Paths.get(TEST_PATH + UUID.randomUUID() + File.separatorChar);
		Files.createDirectories(basePath);

		testFuture = new CompletableFuture<>();
		fileService = new FileService(testFuture::completeExceptionally);
	}

	@Override
	public void afterEach(ExtensionContext extensionContext) throws Exception {
		// stop file-service first, so that it doesn't go insane on cleanup
		fileService.close();
		FileUtils.forceDeleteFiles(basePath.toFile().getAbsolutePath());
	}


	@Override
	public void afterAll(ExtensionContext extensionContext) throws Exception {
		FileUtils.forceDeleteFiles(TEST_PATH);
	}

	public Path basePath() {
		return basePath;
	}

	public CompletableFuture<Path> onFileAppend(File file) throws IOException {
		assert file.toPath().getParent().equals(basePath)
				: "file must be in the " + basePath + " folder, but it's " + file;

		CompletableFuture<Path> appendFuture = newPathFuture();
		fileService.onFileChange(file.toPath(), () -> appendFuture.complete(null));
		return appendFuture;
	}

	/**
	 * Returns a future completed when a new file is created in the {@link #basePath}.
	 * */
	public CompletableFuture<Path> onFileCreate() throws IOException {
		// complete future when file created
        CompletableFuture<Path> fileCreateFuture = newPathFuture();
		fileService.onFileCreate(basePath, fileCreateFuture::complete);
		return fileCreateFuture;
	}

	private CompletableFuture<Path> newPathFuture() {
		CompletableFuture<Path> fileCreateFuture = new CompletableFuture<>();
		testFuture.whenComplete(
				// ignore test-future non-error result, but finish on test-error
				(r, e) -> {
					if(e != null && !fileCreateFuture.isDone()) {
						fileCreateFuture.completeExceptionally(e);
					}
				}
		);
		return fileCreateFuture;
	}

	public FileService fileService() {
		return fileService;
	}
}
