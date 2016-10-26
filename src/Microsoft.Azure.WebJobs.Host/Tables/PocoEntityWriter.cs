// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Host.Bindings;
using Microsoft.Azure.WebJobs.Host.Converters;
using Microsoft.Azure.WebJobs.Host.Protocols;
using Microsoft.Azure.WebJobs.Host.Storage.Table;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json.Linq;
using System.Linq;

namespace Microsoft.Azure.WebJobs.Host.Tables
{
    /// <summary>
    /// The POCO entity writer.
    /// </summary>
    /// <typeparam name="T">The POCO type.</typeparam>
    internal class PocoEntityWriter<T> : ICollector<T>, IAsyncCollector<T>, IWatcher
    {
        private static readonly IConverter<T, ITableEntity> Converter = PocoToTableEntityConverter<T>.Create();

        public PocoEntityWriter(IStorageTable table, TableParameterLog tableStatistics)
        {
            TableEntityWriter = new TableEntityWriter<ITableEntity>(table, tableStatistics);
        }

        public PocoEntityWriter(IStorageTable table)
        {
            TableEntityWriter = new TableEntityWriter<ITableEntity>(table);
        }

        internal TableEntityWriter<ITableEntity> TableEntityWriter { get; set; }

        public void Add(T item)
        {
            AddAsync(item).GetAwaiter().GetResult();
        }

        public Task FlushAsync(CancellationToken cancellationToken)
        {
            return TableEntityWriter.FlushAsync(cancellationToken);
        }

        public Task AddAsync(T item, CancellationToken cancellationToken = default(CancellationToken))
        {
            ITableEntity tableEntity = Converter.Convert(item);
            return TableEntityWriter.AddAsync(tableEntity, cancellationToken);
        }

        public ParameterLog GetStatus()
        {
            return TableEntityWriter.GetStatus();
        }
    }


    internal class JObjectToTableEntityConverter : IConverter<JObject, ITableEntity>
    {
        public static JObjectToTableEntityConverter Instance = new JObjectToTableEntityConverter();

        public ITableEntity Convert(JObject input)
        {
            // $$$ Don't have context...
            return CreateTableEntityFromJObject(null, null, input);
        }

        static string Resolve(string value)
        {
            return value; // $$$ use name resolver
        }


        private DynamicTableEntity CreateTableEntityFromJObject(string partitionKey, string rowKey, JObject entity)
        {
            // any key values specified on the entity override any values
            // specified in the binding
            JProperty keyProperty = entity.Properties().SingleOrDefault(p => string.Compare(p.Name, "partitionKey", StringComparison.OrdinalIgnoreCase) == 0);
            if (keyProperty != null)
            {
                partitionKey = Resolve((string)keyProperty.Value);
                entity.Remove(keyProperty.Name);
            }

            keyProperty = entity.Properties().SingleOrDefault(p => string.Compare(p.Name, "rowKey", StringComparison.OrdinalIgnoreCase) == 0);
            if (keyProperty != null)
            {
                rowKey = Resolve((string)keyProperty.Value);
                entity.Remove(keyProperty.Name);
            }

            DynamicTableEntity tableEntity = new DynamicTableEntity(partitionKey, rowKey);
            foreach (JProperty property in entity.Properties())
            {
                EntityProperty entityProperty = CreateEntityPropertyFromJProperty(property);
                tableEntity.Properties.Add(property.Name, entityProperty);
            }

            return tableEntity;
        }

        private static EntityProperty CreateEntityPropertyFromJProperty(JProperty property)
        {
            switch (property.Value.Type)
            {
                case JTokenType.String:
                    return EntityProperty.GeneratePropertyForString((string)property.Value);
                case JTokenType.Integer:
                    return EntityProperty.GeneratePropertyForInt((int)property.Value);
                case JTokenType.Boolean:
                    return EntityProperty.GeneratePropertyForBool((bool)property.Value);
                case JTokenType.Guid:
                    return EntityProperty.GeneratePropertyForGuid((Guid)property.Value);
                case JTokenType.Float:
                    return EntityProperty.GeneratePropertyForDouble((double)property.Value);
                default:
                    return EntityProperty.CreateEntityPropertyFromObject((object)property.Value);
            }
        }
    }
}
