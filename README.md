A virtual-thread friendly file-append listening service. Allows the creation of InputStreams, which can wait for additional information to be appended into the file (until a configured timeout)

To run, start a FileService instance and instantiate a TailInputStream with the reference to the FileService instance, the absolute name of the file and the timeout in milliseconds, after which an EOF is sent to the API.

For example usage, see TailInputStreamTest in the io-relay project
