using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string bootstrapServers = "localhost:29091,localhost:29092,localhost:29093";
const string topicName = "task-topic";
const int numPartitions = 3;
const int replicationFactor = 3;

Console.WriteLine("=== Kafka Console Tasks ===");
Console.WriteLine("1. Создать топик и заполнить 1000+ сообщений");
Console.WriteLine("2. Достать сообщение с оффсетом 15 из партиции");
Console.WriteLine("3. Переместить оффсет на позицию 10 и читать дальше");
Console.WriteLine("4. Диагностика: проверить текущие оффсеты и состояние топика");
Console.WriteLine("5. Альтернативное чтение: читать с оффсета 10 без consumer group");
Console.Write("Выберите задачу (1-5): ");

var choice = Console.ReadLine();

switch (choice)
{
	case "1":
		await CreateTopicAndFillMessages();
		break;
	case "2":
		await GetMessageFromSpecificOffset();
		break;
	case "3":
		await ResetOffsetsAndRead();
		break;
	case "4":
		await DiagnoseTopicAndOffsets();
		break;
	case "5":
		await ReadFromOffset10WithoutGroup();
		break;
	default:
		break;
}

async Task CreateTopicAndFillMessages()
{
	Console.WriteLine($"\n=== Создание топика '{topicName}' и заполнение сообщениями ===");
	
	var adminConfig = new AdminClientConfig { BootstrapServers = bootstrapServers };
	using var adminClient = new AdminClientBuilder(adminConfig).Build();

	try
	{
		try
		{
			await adminClient.DeleteTopicsAsync([topicName]);
			Console.WriteLine($"Топик '{topicName}' удален");
			await Task.Delay(2000); // Ждем удаления
		}
		catch
		{
			Console.WriteLine($"Топик '{topicName}' не существовал");
		}
		
		await adminClient.CreateTopicsAsync([
			new TopicSpecification
			{
				Name = topicName,
				NumPartitions = numPartitions,
				ReplicationFactor = replicationFactor,
				Configs = new Dictionary<string, string> { ["min.insync.replicas"] = "2" }
			}
		]);
		Console.WriteLine($"Топик '{topicName}' создан с {numPartitions} партициями");
	}
	catch (Exception ex)
	{
		Console.WriteLine($"Ошибка при работе с топиком: {ex.Message}");
	}
	
	var producerConfig = new ProducerConfig
	{
		BootstrapServers = bootstrapServers,
		MessageTimeoutMs = 5000,
		Acks = Acks.All
	};

	using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

	Console.WriteLine("Отправка 1000+ сообщений...");
	var tasks = new List<Task<DeliveryResult<string, string>>>();

	for (int i = 0; i < 1200; i++)
	{
		var message = new Message<string, string>
		{
			Key = $"key-{i}",
			Value = $"Message #{i} created at {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff}"
		};

		var task = producer.ProduceAsync(topicName, message);
		tasks.Add(task);

		if (i % 100 == 0)
		{
			Console.WriteLine($"Отправлено {i} сообщений...");
		}
	}

	var results = await Task.WhenAll(tasks);
	Console.WriteLine($"\nУспешно отправлено {results.Length} сообщений!");
	
	var partitionCounts = results.GroupBy(r => r.Partition.Value)
								.ToDictionary(g => g.Key, g => g.Count());
	
	Console.WriteLine("\nРаспределение сообщений по партициям:");
	foreach (var kvp in partitionCounts.OrderBy(x => x.Key))
	{
		Console.WriteLine($"  Партиция {kvp.Key}: {kvp.Value} сообщений");
	}
}

async Task GetMessageFromSpecificOffset()
{
	Console.WriteLine($"\n=== Чтение сообщения с оффсетом 15 ===");
	
	Console.Write("Введите номер партиции (0-2): ");
	if (!int.TryParse(Console.ReadLine(), out int partitionNumber) || 
		partitionNumber < 0 || partitionNumber >= numPartitions)
	{
		Console.WriteLine("Неверный номер партиции!");
		return;
	}

	var consumerConfig = new ConsumerConfig
	{
		BootstrapServers = bootstrapServers,
		GroupId = $"offset-reader-{Guid.NewGuid()}",
		AutoOffsetReset = AutoOffsetReset.Earliest,
		EnableAutoCommit = false
	};

	using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();

	var topicPartition = new TopicPartition(topicName, new Partition(partitionNumber));
	var topicPartitionOffset = new TopicPartitionOffset(topicPartition, new Offset(15));
	consumer.Assign(topicPartitionOffset);
	
	Console.WriteLine($"Ищем сообщение в партиции {partitionNumber} с оффсетом 15...");

	try
	{
		var consumeResult = consumer.Consume(TimeSpan.FromSeconds(10));
		
		if (consumeResult != null)
		{
			Console.WriteLine($"\nНайдено сообщение:");
			Console.WriteLine($"   Партиция: {consumeResult.Partition.Value}");
			Console.WriteLine($"   Оффсет: {consumeResult.Offset.Value}");
			Console.WriteLine($"   Ключ: {consumeResult.Message.Key}");
			Console.WriteLine($"   Значение: {consumeResult.Message.Value}");
			Console.WriteLine($"   Timestamp: {consumeResult.Message.Timestamp.UtcDateTime:yyyy-MM-dd HH:mm:ss.fff}");
		}
		else
		{
			Console.WriteLine("Сообщение с оффсетом 15 не найдено (возможно партиция содержит меньше сообщений)");
		}
	}
	catch (Exception ex)
	{
		Console.WriteLine($"Ошибка при чтении: {ex.Message}");
	}
}

async Task ResetOffsetsAndRead()
{
	Console.WriteLine($"\n=== Сброс оффсетов на позицию 10 и чтение ===");
	
	const string consumerGroup = "offset-reset-group";
	
	var adminConfig = new AdminClientConfig { BootstrapServers = bootstrapServers };
	using var adminClient = new AdminClientBuilder(adminConfig).Build();

	List<int> partitions;

	try
	{
		var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10));
		partitions = metadata.Topics[0].Partitions.Select(p => p.PartitionId).ToList();

		Console.WriteLine($"Найдено партиций: {partitions.Count}");
		
		var tempConsumerConfig = new ConsumerConfig
		{
			BootstrapServers = bootstrapServers,
			GroupId = consumerGroup,
			EnableAutoCommit = false,
			SessionTimeoutMs = 30000,
			MaxPollIntervalMs = 300000
		};

		using var tempConsumer = new ConsumerBuilder<string, string>(tempConsumerConfig).Build();
		
		tempConsumer.Subscribe(topicName);
		
		bool allPartitionsAssigned = false;
		var timeout = DateTime.UtcNow.AddSeconds(30);
		
		Console.WriteLine("Ожидание назначения всех партиций...");
		
		while (!allPartitionsAssigned && DateTime.UtcNow < timeout)
		{
			tempConsumer.Consume(TimeSpan.FromMilliseconds(500));
			var assignment = tempConsumer.Assignment;
			
			Console.WriteLine($"Текущее назначение: {assignment.Count} партиций ({string.Join(", ", assignment.Select(a => a.Partition.Value))})");
			
			if (assignment.Count == partitions.Count)
			{
				allPartitionsAssigned = true;
				Console.WriteLine($"Получено назначение всех {assignment.Count} партиций");
			}
			else
			{
				Console.WriteLine($"Ждем назначения остальных партиций...");
			}
		}

		if (!allPartitionsAssigned)
		{
			Console.WriteLine("Не удалось получить назначение всех партиций, пробуем принудительное назначение");
			
			// Принудительно назначаем все партиции
			var forceAssignments = partitions.Select(p => new TopicPartition(topicName, p)).ToList();
			tempConsumer.Assign(forceAssignments);
			Console.WriteLine($"Принудительно назначено {forceAssignments.Count} партиций");
		}
		
		var offsetsToCommit = partitions.Select(partitionId => 
			new TopicPartitionOffset(new TopicPartition(topicName, partitionId), new Offset(10))
		).ToList();
		
		tempConsumer.Commit(offsetsToCommit);
		Console.WriteLine("Оффсеты всех партиций сброшены на позицию 10");

		foreach (var tpo in offsetsToCommit)
		{
			Console.WriteLine($"Партиция {tpo.Partition.Value}: оффсет установлен на {tpo.Offset.Value}");
		}
		
		Console.WriteLine("\nПроверка установленных оффсетов:");
		var committedOffsets = tempConsumer.Committed(
			partitions.Select(p => new TopicPartition(topicName, p)).ToList(),
			TimeSpan.FromSeconds(10));
		
		foreach (var offset in committedOffsets)
		{
			Console.WriteLine($"Партиция {offset.Partition.Value}: committed offset = {offset.Offset.Value}");
		}
	}
	catch (Exception ex)
	{
		Console.WriteLine($"Ошибка при сбросе оффсетов: {ex.Message}");
		return;
	}
	
	await Task.Delay(2000);
	
	Console.WriteLine("\n=== Чтение сообщений начиная с оффсета 10 ===");
	
	var readerConfig = new ConsumerConfig
	{
		BootstrapServers = bootstrapServers,
		GroupId = consumerGroup,
		AutoOffsetReset = AutoOffsetReset.Earliest,
		EnableAutoCommit = true,
		MaxPollIntervalMs = 300000,
		SessionTimeoutMs = 30000
	};

	using var reader = new ConsumerBuilder<string, string>(readerConfig).Build();
	
	var readerAssignments = partitions.Select(p => new TopicPartition(topicName, p)).ToList();
	reader.Assign(readerAssignments);
	Console.WriteLine($"Принудительно назначено {readerAssignments.Count} партиций для чтения");

	var messagesRead = 0;
	var maxMessages = 30;

	Console.WriteLine($"Читаем первые {maxMessages} сообщений после оффсета 10...\n");

	try
	{
		while (messagesRead < maxMessages)
		{
			var consumeResult = reader.Consume(TimeSpan.FromSeconds(5));
			
			if (consumeResult == null)
			{
				Console.WriteLine("Больше сообщений нет (таймаут)");
				break;
			}

			messagesRead++;
			Console.WriteLine($"[{messagesRead:D2}] Партиция: {consumeResult.Partition.Value}, " +
							$"Оффсет: {consumeResult.Offset.Value}, " +
							$"Ключ: {consumeResult.Message.Key}, " +
							$"Значение: {consumeResult.Message.Value}");
			
			if (messagesRead % 5 == 0)
			{
				reader.Commit(consumeResult);
				Console.WriteLine($"  → Коммит выполнен на оффсете {consumeResult.Offset.Value}");
			}
		}
	}
	catch (Exception ex)
	{
		Console.WriteLine($"Ошибка при чтении: {ex.Message}");
	}

	Console.WriteLine($"\nПрочитано {messagesRead} сообщений");
}

async Task DiagnoseTopicAndOffsets()
{
	Console.WriteLine($"\n=== Диагностика: проверить текущие оффсеты и состояние топика ===");
	
	var adminConfig = new AdminClientConfig { BootstrapServers = bootstrapServers };
	using var adminClient = new AdminClientBuilder(adminConfig).Build();

	try
	{
		var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10));
		var partitions = metadata.Topics[0].Partitions.Select(p => p.PartitionId).ToList();

		Console.WriteLine($"Найдено партиций: {partitions.Count}");
		
		var consumerConfig = new ConsumerConfig
		{
			BootstrapServers = bootstrapServers,
			GroupId = "offset-reset-group",
			EnableAutoCommit = false
		};

		using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
		
		Console.WriteLine("\nТекущие committed оффсеты в consumer group 'offset-reset-group':");
		var topicPartitions = partitions.Select(p => new TopicPartition(topicName, p)).ToList();
		var committedOffsets = consumer.Committed(topicPartitions, TimeSpan.FromSeconds(10));
		
		foreach (var offset in committedOffsets)
		{
			Console.WriteLine($"   Партиция {offset.Partition.Value}: committed offset = {offset.Offset.Value}");
		}
		
		Console.WriteLine("\nПоследние оффсеты (конец партиций):");
		var watermarks = topicPartitions.Select(tp => 
		{
			var watermark = consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(5));
			return new { Partition = tp.Partition.Value, HighWatermark = watermark.High.Value };
		}).ToList();

		foreach (var wm in watermarks)
		{
			Console.WriteLine($"   Партиция {wm.Partition}: последний оффсет = {wm.HighWatermark - 1} (всего сообщений: {wm.HighWatermark})");
		}
		
		Console.WriteLine("\nПроверка сообщений в каждой партиции (оффсеты 0, 10, 15):");
		
		foreach (var partitionId in partitions)
		{
			Console.WriteLine($"\n  Партиция {partitionId}:");
			
			var offsetsToCheck = new[] { 0, 10, 15 };
			
			foreach (var offsetToCheck in offsetsToCheck)
			{
				try
				{
					var tpo = new TopicPartitionOffset(new TopicPartition(topicName, partitionId), new Offset(offsetToCheck));
					consumer.Assign(tpo);
					
					var result = consumer.Consume(TimeSpan.FromSeconds(2));
					if (result != null)
					{
						Console.WriteLine($"    Оффсет {offsetToCheck}: ключ={result.Message.Key}, значение={result.Message.Value}");
					}
					else
					{
						Console.WriteLine($"    Оффсет {offsetToCheck}: сообщение не найдено");
					}
				}
				catch (Exception ex)
				{
					Console.WriteLine($"    Оффсет {offsetToCheck}: ошибка - {ex.Message}");
				}
			}
		}
	}
	catch (Exception ex)
	{
		Console.WriteLine($"Ошибка при диагностике: {ex.Message}");
	}
}

async Task ReadFromOffset10WithoutGroup()
{
	Console.WriteLine($"\n=== Альтернативное чтение: читать с оффсета 10 без consumer group ===");
	
	var consumerConfig = new ConsumerConfig
	{
		BootstrapServers = bootstrapServers,
		GroupId = $"temp-reader-{Guid.NewGuid()}",
		AutoOffsetReset = AutoOffsetReset.Earliest,
		EnableAutoCommit = false,
		MaxPollIntervalMs = 300000,
		SessionTimeoutMs = 30000
	};

	using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
	
	var partitionAssignments = new List<TopicPartitionOffset>
	{
		new(new TopicPartition(topicName, 0), new Offset(10)),
		new(new TopicPartition(topicName, 1), new Offset(10)),
		new(new TopicPartition(topicName, 2), new Offset(10))
	};
	
	consumer.Assign(partitionAssignments);
	Console.WriteLine($"Принудительно назначено {partitionAssignments.Count} партиций с оффсетом 10");

	var messagesRead = 0;
	var maxMessages = 30;

	Console.WriteLine($"Читаем первые {maxMessages} сообщений начиная с оффсета 10...\n");

	try
	{
		while (messagesRead < maxMessages)
		{
			var consumeResult = consumer.Consume(TimeSpan.FromSeconds(5));
			
			if (consumeResult == null)
			{
				Console.WriteLine("Больше сообщений нет (таймаут)");
				break;
			}

			messagesRead++;
			Console.WriteLine($"[{messagesRead:D2}] Партиция: {consumeResult.Partition.Value}, " +
							$"Оффсет: {consumeResult.Offset.Value}, " +
							$"Ключ: {consumeResult.Message.Key}, " +
							$"Значение: {consumeResult.Message.Value}");
			
			if (messagesRead % 5 == 0)
			{
				Console.WriteLine($"  → Прочитано {messagesRead} сообщений");
			}
		}
	}
	catch (Exception ex)
	{
		Console.WriteLine($"Ошибка при чтении: {ex.Message}");
	}

	Console.WriteLine($"\nПрочитано {messagesRead} сообщений");
}