using System;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.DataMovement;
using Microsoft.Extensions.Configuration;
using System.IO;
namespace DataMovementLibrary {
    public class Program {
        public IConfiguration config { get; set; }
        static Program prg = null;


        public static Program getInstance() {
            if (prg != null) {
                return prg;
            }
            else {
                prg = new Program();
                return prg;
            }
        }


        public static async Task Main(string[] args) {
            Program p = Program.getInstance();
            IConfiguration config = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .AddCommandLine(args)
            .Build();
            p.config = config;

            Console.WriteLine("Enter Storage account name:");
            string accountName = config.GetValue<String>("StorageAccount"); //Console.ReadLine();

            Console.WriteLine("\nEnter Storage account Key:");
            string accountKey = config.GetValue<String>("StorageKey"); //Console.ReadLine();

            string storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=" + accountName + ";AccountKey=" + accountKey;
            CloudStorageAccount account = CloudStorageAccount.Parse(storageConnectionString);

            ExecuteChoice(account);
        }

        public static string GetSourcePath() {
            string sourcePath = Program.getInstance().config.GetValue<String>("SourceDirPath"); //Console.ReadLine();
            Console.WriteLine($"Reading from source: {sourcePath}");

            return sourcePath;
        }

        public static CloudBlockBlob GetBlob(CloudStorageAccount account, string fileName = "") {
            CloudBlobClient blobClient = account.CreateCloudBlobClient();
            Program p = Program.getInstance();

            string containerName = p.config.GetValue<String>("ContainerName");
            Console.WriteLine($"Writing to blob container: {containerName}");
            
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            container.CreateIfNotExistsAsync().Wait();

            string blobName = fileName;
            CloudBlockBlob blob = container.GetBlockBlobReference(blobName);

            return blob;
        }

        static void printDefaultInstructions() {
            Console.Clear();
            Console.WriteLine("");
            Console.WriteLine("---------");
            Console.WriteLine("\nWhat type of transfer would you like to execute?\n1. Local file --> Azure Blob\n2. Local directory --> Azure Blob directory\n3. Azure Blob --> Azure Blob");
            Console.WriteLine("---------");
            Console.WriteLine("Please enter selection");
        }
        public static void ExecuteChoice(CloudStorageAccount account) {
            printDefaultInstructions();
            string input = Console.ReadLine().ToUpper();
            while ((input != "")) {
                switch (input.Trim()) {
                    case "1":
                        TransferLocalFileToAzureBlob(account).Wait();
                        break;
                    case "2":
                        TransferLocalDirectoryToAzureBlobDirectory(account).Wait();
                        break;
                    case "3":
                        TransferAzureBlobToAzureBlob(account).Wait();
                        break;
                }
                printDefaultInstructions();
            }
        }

        //Sets the number of Parallel Operations that DML will support
        //As a general rule, use 8 x (# of Cores)
        public static void SetNumberOfParallelOperations() {
            Program p = Program.getInstance();
            string parallelOperations = p.config.GetValue<String>("ParallelOperations");
            Console.WriteLine($"Using {parallelOperations} Parallel Operations...");
         
            TransferManager.Configurations.ParallelOperations = int.Parse(parallelOperations);
        }

        //Upload a local file to Azure Blob Storage
        public static async Task TransferLocalFileToAzureBlob(CloudStorageAccount account) {
            string localFilePath = GetSourcePath();
            string fileName = Path.GetFileName(localFilePath).TrimStart('\\');

            CloudBlockBlob blob = GetBlob(account, fileName);
            TransferCheckpoint checkpoint = null;
            SingleTransferContext context = GetSingleTransferContext(checkpoint);
            Console.WriteLine("\nTransfer started...\n");
            Stopwatch stopWatch = Stopwatch.StartNew();
            await TransferManager.UploadAsync(localFilePath, blob, null, context);
            stopWatch.Stop();
            Console.WriteLine("\nTransfer operation completed in " + stopWatch.Elapsed.TotalSeconds + " seconds.");
            Console.WriteLine("Press Enter to Continue..");
            Console.ReadLine();
            ExecuteChoice(account);
        }

        //Gets a reference to a CloudBlob from Azure
        public static CloudBlobDirectory GetBlobDirectory(CloudStorageAccount account) {
            CloudBlobClient blobClient = account.CreateCloudBlobClient();
            string containerName = Program.getInstance().config.GetValue<string>("TargetContainer");

            Console.WriteLine($"Uploading to Blob container: {containerName}");
            
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            container.CreateIfNotExistsAsync().Wait();

            CloudBlobDirectory blobDirectory = container.GetDirectoryReference("");

            return blobDirectory;
        }

        //Transfer a local directory to Azure
        public static async Task TransferLocalDirectoryToAzureBlobDirectory(CloudStorageAccount account) {
            string localDirectoryPath = GetSourcePath();
            CloudBlobDirectory blobDirectory = GetBlobDirectory(account);
            TransferCheckpoint checkpoint = null;
            DirectoryTransferContext context = GetDirectoryTransferContext(checkpoint);
            CancellationTokenSource cancellationSource = new CancellationTokenSource();
            Console.WriteLine("\nTransfer started...\nPress 'c' to temporarily cancel your transfer...\n");

            Stopwatch stopWatch = Stopwatch.StartNew();
            Task task;
            ConsoleKeyInfo keyinfo;
            UploadDirectoryOptions options = new UploadDirectoryOptions() {
                Recursive = true
            };

            try {
                task = TransferManager.UploadDirectoryAsync(localDirectoryPath, blobDirectory, options, context, cancellationSource.Token);
                while (!task.IsCompleted) {
                    if (Console.KeyAvailable) {
                        keyinfo = Console.ReadKey(true);
                        if (keyinfo.Key == ConsoleKey.C) {
                            cancellationSource.Cancel();
                        }
                    }
                }
                await task;
            }
            catch (Exception e) {
                Console.WriteLine("\nThe transfer is canceled: {0}", e.Message);
            }

            if (cancellationSource.IsCancellationRequested) {
                Console.WriteLine("\nTransfer will resume in 3 seconds...");
                Thread.Sleep(1000); //Pause for 1 Second
                checkpoint = context.LastCheckpoint;
                context = GetDirectoryTransferContext(checkpoint);
                Console.WriteLine("\nResuming transfer...\n");
                await TransferManager.UploadDirectoryAsync(localDirectoryPath, blobDirectory, options, context);
            }

            stopWatch.Stop();
            Console.WriteLine("\nTransfer operation completed in " + stopWatch.Elapsed.TotalSeconds + " seconds.");
            Console.WriteLine("Press Enter to continue...");
            Console.ReadLine();
            ExecuteChoice(account);
        }


        //Transfer from an Azure Blob to another Azure Blob, using a Server Side Async Copy
        public static async Task TransferAzureBlobToAzureBlob(CloudStorageAccount account) {
            CloudBlockBlob sourceBlob = GetBlob(account);
            CloudBlockBlob destinationBlob = GetBlob(account);
            TransferCheckpoint checkpoint = null;
            SingleTransferContext context = GetSingleTransferContext(checkpoint);
            CancellationTokenSource cancellationSource = new CancellationTokenSource();
            Console.WriteLine("\nTransfer started...\nPress 'c' to temporarily cancel your transfer...\n");

            Stopwatch stopWatch = Stopwatch.StartNew();
            Task task;
            ConsoleKeyInfo keyinfo;
            try {
                task = TransferManager.CopyAsync(sourceBlob, destinationBlob, CopyMethod.ServiceSideAsyncCopy, null, context, cancellationSource.Token);
                while (!task.IsCompleted) {
                    if (Console.KeyAvailable) {
                        keyinfo = Console.ReadKey(true);
                        if (keyinfo.Key == ConsoleKey.C) {
                            cancellationSource.Cancel();
                        }
                    }
                }
                await task;
            }
            catch (Exception e) {
                Console.WriteLine("\nThe transfer is canceled: {0}", e.Message);
            }

            if (cancellationSource.IsCancellationRequested) {
                Console.WriteLine("\nTransfer will resume in 3 seconds...");
                Thread.Sleep(3000);
                checkpoint = context.LastCheckpoint;
                context = GetSingleTransferContext(checkpoint);
                Console.WriteLine("\nResuming transfer...\n");
                await TransferManager.CopyAsync(sourceBlob, destinationBlob, false, null, context, cancellationSource.Token);
            }

            stopWatch.Stop();
            Console.WriteLine("\nTransfer operation completed in " + stopWatch.Elapsed.TotalSeconds + " seconds.");
            Console.WriteLine("Press Enter to continue...");
            Console.ReadLine();
            ExecuteChoice(account);
        }


        //Used to track Transfer of a Single File
        public static SingleTransferContext GetSingleTransferContext(TransferCheckpoint checkpoint) {
            SingleTransferContext context = new SingleTransferContext(checkpoint);

            context.ProgressHandler = new Progress<TransferStatus>((progress) =>
            {
                Console.Write("\rBytes transferred: {0}", progress.BytesTransferred);
            });

            return context;
        }

        //Used to track transfer of a Directory
        public static DirectoryTransferContext GetDirectoryTransferContext(TransferCheckpoint checkpoint) {
            DirectoryTransferContext context = new DirectoryTransferContext(checkpoint);

            context.ProgressHandler = new Progress<TransferStatus>((progress) =>
            {
                Console.Write("\rBytes transferred: {0}", progress.BytesTransferred);
            });

            return context;
        }
    }
}
