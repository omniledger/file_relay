A virtual-thread friendly file-append listening service. Allows the creation of InputStreams, which can wait for additional information to be appended into the file (until a configured timeout)

To run, start a (FileService)[https://github.com/omniledger/file_relay/blob/main/core/src/main/java/io/omniledger/filerelay/FileService.java] instance and instantiate a (TailInputStream)[https://github.com/omniledger/file_relay/blob/main/io-relay/src/main/java/io/omniledger/filerelay/io/TailInputStream.java] with the reference to the FileService instance, the absolute name of the file and the timeout in milliseconds, after which an EOF is sent to the API.

For example usage, see [TailInputStreamTest](https://github.com/omniledger/file_relay/blob/main/io-relay/src/test/java/io/omniledger/filerelay/io/TailInputStreamTest.java) in the (io-relay)[https://github.com/omniledger/file_relay/tree/main/io-relay] project
