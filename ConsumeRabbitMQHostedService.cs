﻿/*************************************************************************************************************************
 *                               DEVELOPMENT BY      : NURMAN HARIYANTO - PT.LSKK & PPTIK                                *
 *                               VERSION             : 2                                                                 *
 *                               TYPE APPLICATION    : WORKER                                                            *
 * DESCRIPTION         : GET DATA FROM MQTT (OUTPUT DEVICE) CHECK TO DB RULES AND SEND BACK (INPUT DEVICE) IF DATA EXIST *
 *************************************************************************************************************************/

 /*************************************************************************************************************************
 *                        UPDATE DEVELOPMENT BY      : M. AJI PERDANA - PT.LSKK & PPTIK, FEBRUARY 2025                   *
 *                                  VERSION          : 3                                                                 *
 *                               TYPE APPLICATION    : WORKER                                                            *
 * DESCRIPTION         : REFACTORING CODE INTO NEW DB STRUCTURE, FIXING ITERATION WHEN INSERT TO LOGS, AND FIXING BUG    *
 *************************************************************************************************************************/

namespace worker_smarthome_cloud_server
{
   using System.Configuration;
   using System.Threading;
   using System.Threading.Tasks;
   using System.Text;
   using Microsoft.Extensions.Hosting;
   using Microsoft.Extensions.Logging;
   using RabbitMQ.Client;
   using RabbitMQ.Client.Events;
   using Microsoft.Data.Sqlite;
   using System;

   public class ConsumeRabbitMQHostedService: BackgroundService
   {
      private readonly ILogger _logger;
      private IConnection _connection;
      private IModel _channel;

      private static string RMQHost = ConfigurationManager.AppSettings["RMQHost"];
      private static string RMQVHost = ConfigurationManager.AppSettings["RMQVHost"];
      private static string RMQUsername = ConfigurationManager.AppSettings["RMQUsername"];
      private static string RMQPassword = ConfigurationManager.AppSettings["RMQPassword"];
      private static string RMQQueue = ConfigurationManager.AppSettings["RMQQueue"];
      private static string RMQExc = ConfigurationManager.AppSettings["RMQExc"];
      private static string RMQPubRoutingKey = ConfigurationManager.AppSettings["RMQPubRoutingKey"];
      private static string DBPath = ConfigurationManager.AppSettings["DBPath"];
      private static string InputGuid = "";
      private static string ValueInput = "";
      private static string OutputGuid = "";
      private static string ValueOutput = "";
      private static string MessageSend = "";
      private static string DeviceName = "";

      public ConsumeRabbitMQHostedService(ILoggerFactory loggerFactory)
      {
         _logger = loggerFactory.CreateLogger < ConsumeRabbitMQHostedService > ();
         InitRabbitMQ();
      }

      private void InitRabbitMQ()
      {
         var factory = new ConnectionFactory
         {
            HostName = RMQHost,
            VirtualHost = RMQVHost,
            UserName = RMQUsername,
            Password = RMQPassword
         };

         // create connection
         _connection = factory.CreateConnection();

         // create channel
         _channel = _connection.CreateModel();

         _connection.ConnectionShutdown += RabbitMQ_ConnectionShutdown;
      }

      protected override Task ExecuteAsync(CancellationToken stoppingToken)
      {
         stoppingToken.ThrowIfCancellationRequested();

         var consumer = new EventingBasicConsumer(_channel);
         consumer.Received += (ch, ea) =>
         {
            // received message
            var body = ea.Body.ToArray();
            var content = System.Text.Encoding.UTF8.GetString(body);
            // handle the received message
            HandleMessageToDB(content);
            _channel.BasicAck(ea.DeliveryTag, true);
         };

         consumer.Shutdown += OnConsumerShutdown;

         _channel.BasicConsume(RMQQueue, false, consumer);
         return Task.CompletedTask;
      }

      private void HandleMessageToDB(string content)
      {
         var connectionStringBuilder = new SqliteConnectionStringBuilder();
         connectionStringBuilder.DataSource = DBPath;
         
         var connectionDB = new SqliteConnection(connectionStringBuilder.ConnectionString);

         string[] dataParsing = content.Split('#');
         foreach(var datas in dataParsing)
         {
            InputGuid = dataParsing[0];
            ValueInput = dataParsing[1];
         }
         
         TimeZoneInfo asia = TimeZoneInfo.FindSystemTimeZoneById("SE Asia Standard Time");
         DateTime now = DateTime.UtcNow;
         DateTime TimeStamp = TimeZoneInfo.ConvertTimeFromUtc(now, asia);

         _logger.LogInformation($"GuidInput: {InputGuid} - ValueInput: {ValueInput} - TimeStamp: {TimeStamp}");

         connectionDB.Open();
            var selectCmd = connectionDB.CreateCommand();
            selectCmd.CommandText = "SELECT * FROM rule_devices WHERE input_guid=@Guidinput AND input_value=@Valueinput";
            selectCmd.Parameters.AddWithValue("@Guidinput", InputGuid);
            selectCmd.Parameters.AddWithValue("@Valueinput", ValueInput);
            using(var reader = selectCmd.ExecuteReader())
            {
               while (reader.Read())
               {
                  
                  // for (int i = 0; i < reader.FieldCount; i++)
                  // {
                  //    Console.WriteLine($"Column {i}: {reader.GetValue(i)}");
                  // }
                  
                  OutputGuid = reader.GetString(3);
                  ValueOutput = reader.GetString(4);

                  MessageSend = OutputGuid + "#" + ValueOutput;
                  _logger.LogInformation($"MessageSend: {MessageSend}");

                  var selectRegistrationCmd = connectionDB.CreateCommand();
                  selectRegistrationCmd.CommandText = "SELECT * FROM registrations WHERE guid=@Guidinput";
                  selectRegistrationCmd.Parameters.AddWithValue("@Guidinput", InputGuid);

                  using (var reader2 = selectRegistrationCmd.ExecuteReader())
                  {
                     while (reader2.Read())
                     {
                        DeviceName = reader2.GetString(5);
                     }
                  }

                  using (var transaction = connectionDB.BeginTransaction()) 
                  {
                     try 
                     {
                        using (var checkGuidCmd = connectionDB.CreateCommand())
                        {
                           checkGuidCmd.CommandText = "SELECT COUNT(*) FROM registrations WHERE guid = @guidOutput";
                           checkGuidCmd.Parameters.AddWithValue("@guidOutput", OutputGuid);
                           var count = Convert.ToInt32(checkGuidCmd.ExecuteScalar());

                           if (count == 0)
                           {
                              _logger.LogError($"Foreign key violation: guidOutput {OutputGuid} not found in registrations");
                              transaction.Rollback();
                              return;
                           }
                        }

                        using (var updateRegistration = connectionDB.CreateCommand()) 
                        {
                           updateRegistration.Transaction = transaction;

                           if (ValueInput == "0") 
                           {
                              updateRegistration.CommandText = @"
                                 UPDATE registrations SET status=true, updated_at=@timestamp WHERE guid=@guidInput;
                                 UPDATE registrations SET status=true, updated_at=@timestamp WHERE guid=@guidOutput;
                              ";
                           } 
                           else if (ValueInput == "1") 
                           {
                              updateRegistration.CommandText = @"
                                 UPDATE registrations SET status=false, updated_at=@timestamp WHERE guid=@guidInput;
                                 UPDATE registrations SET status=false, updated_at=@timestamp WHERE guid=@guidOutput;
                              ";
                           }

                           updateRegistration.Parameters.AddWithValue("@guidInput", InputGuid);
                           updateRegistration.Parameters.AddWithValue("@guidOutput", OutputGuid);
                           updateRegistration.Parameters.AddWithValue("@timestamp", TimeStamp);
                           
                           updateRegistration.ExecuteNonQuery();
                        }

                        using (var insertCmd = connectionDB.CreateCommand()) 
                        {
                           insertCmd.Transaction = transaction;
                           insertCmd.CommandText = @"
                              INSERT INTO logs (input_guid, input_name, input_value, output_guid, output_value, time)
                              VALUES (@inputguid, @devicename, @valueinput, @outputguid, @outputvalue, @timestamp);
                           ";

                           insertCmd.Parameters.AddWithValue("@inputguid", InputGuid);
                           insertCmd.Parameters.AddWithValue("@devicename", DeviceName);
                           insertCmd.Parameters.AddWithValue("@valueinput", ValueInput);
                           insertCmd.Parameters.AddWithValue("@outputguid", OutputGuid);
                           insertCmd.Parameters.AddWithValue("@outputvalue", ValueOutput);
                           insertCmd.Parameters.AddWithValue("@timestamp", TimeStamp);
                           
                           insertCmd.ExecuteNonQuery();
                        }

                        transaction.Commit();
                        _logger.LogInformation($"Success inserting data to DB");
                     }
                     catch (Exception ex) 
                     {
                        transaction.Rollback();
                        _logger.LogError($"Database transaction failed: {ex.Message} 💥");
                     }
                  }

                  _channel.BasicPublish(
                     exchange: "amq.topic",
                     routingKey: "Aktuator",
                     basicProperties: null,
                     body: Encoding.UTF8.GetBytes(MessageSend)
                  );
               }
            }
         connectionDB.Close();
         _logger.LogInformation("Sucess Send Data");
      }

      private void RabbitMQ_ConnectionShutdown(object sender, ShutdownEventArgs e)
      {
         _logger.LogInformation($"connection shut down {e.ReplyText}");
      }

      private void OnConsumerShutdown(object sender, ShutdownEventArgs e)
      {
         _logger.LogInformation($"consumer shutdown {e.ReplyText}");
      }

      public override void Dispose()
      {
         _channel.Close();
         _connection.Close();
         base.Dispose();
      }
   }
}