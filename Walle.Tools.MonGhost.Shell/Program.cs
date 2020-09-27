using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.IO;
using System.Linq;
using System.Threading;

namespace Walle.Tools.MonGhost
{
    class Program
    {
        static async System.Threading.Tasks.Task Main(string[] args)
        {
            Console.WriteLine("Start Migration:");
            Console.WriteLine("------------------------------------------------");
            Console.WriteLine($"Source Connect:{ Configurations.DataBase } \t ConnectionString:{ Configurations.Source}");
            MongoClient sourceClient = new MongoClient(Configurations.Source);
            var sourceDatabase = sourceClient.GetDatabase(Configurations.DataBase);
            Console.WriteLine("Source Connect Ok!");

            Console.WriteLine("------------------------------------------------");
            Console.WriteLine($"Target Connect:{ Configurations.DataBase } \t ConnectionString:{ Configurations.Target}");
            MongoClient targetClient = new MongoClient(Configurations.Target);
            var targetDatabase = targetClient.GetDatabase(Configurations.DataBase);
            Console.WriteLine("Target Connect Ok!");
            Console.WriteLine("------------------------------------------------");
            Console.WriteLine("Press Any Key Start:");
            Console.ReadKey();
            Console.WriteLine($"Fetch Source Collections ...");
            var sourceCollectionNames = sourceDatabase.ListCollectionNames()?.ToList();

            if (sourceCollectionNames != null && sourceCollectionNames.Any())
            {
                Console.WriteLine($"Fetched {sourceCollectionNames.Count()} Collections ...");

                var path = $"{Environment.GetFolderPath(Environment.SpecialFolder.DesktopDirectory)}\\DataBaseMigration\\{DateTime.Now:yyyyMMddHHmmss}";
                Directory.CreateDirectory(path);
                Console.WriteLine($"Data Store:{path}");

                foreach (var name in sourceCollectionNames)
                {
                    Console.WriteLine($"Migration Collection:{name}  ...");
                    Thread.Sleep(100);
                    if (targetDatabase.GetCollection<Object>(name) != null)
                    {
                        Console.WriteLine($"  Drop Target Collection:{name}  ...");
                        targetDatabase.DropCollection(name);

                        Console.WriteLine($"  Create Target Collection:{name} ...");
                        targetDatabase.CreateCollection(name);

                        Console.WriteLine($"  Read Source Collection {name} 's Documents ...");
                        var docs = sourceDatabase.GetCollection<object>(name).AsQueryable().ToList();
                        var index = 0;
                        foreach (var doc in docs)
                        {
                            index++;
                            Console.WriteLine($"    Insert {name} Data {index}");
                            targetDatabase.GetCollection<Object>(name).InsertOne(doc);
                            Console.Write($"    Insert Ok;");

                            Directory.CreateDirectory($"{path}\\{name}\\");
                            await File.AppendAllTextAsync($"{path}\\{ name}\\{ index}.json", doc.ToJson());
                            Console.WriteLine($"    Saved To:{path}\\{ name}\\{ index}.json");
                        }
                    }
                }
                Console.WriteLine("------------------------------------------------");
                Console.WriteLine("Finished");
                Console.WriteLine("Press Any Key exist.");
                Console.ReadKey();


            }
        }
    }
}
