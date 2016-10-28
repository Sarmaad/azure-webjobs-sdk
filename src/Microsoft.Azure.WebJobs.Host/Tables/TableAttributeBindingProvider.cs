﻿// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Host.Bindings;
using Microsoft.Azure.WebJobs.Host.Executors;
using Microsoft.Azure.WebJobs.Host.Storage;
using Microsoft.Azure.WebJobs.Host.Storage.Table;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json.Linq;

namespace Microsoft.Azure.WebJobs.Host.Tables
{
    internal class TableAttributeBindingProvider : IBindingProvider
    {
        private readonly IStorageTableArgumentBindingProvider _tableBindingProvider;
        private readonly ITableEntityArgumentBindingProvider _entityBindingProvider;

        private readonly INameResolver _nameResolver;
        private readonly IStorageAccountProvider _accountProvider;

        private TableAttributeBindingProvider(INameResolver nameResolver, IStorageAccountProvider accountProvider, IExtensionRegistry extensions)
        {
            if (accountProvider == null)
            {
                throw new ArgumentNullException("accountProvider");
            }

            if (extensions == null)
            {
                throw new ArgumentNullException("extensions");
            }

            _nameResolver = nameResolver;
            _accountProvider = accountProvider;

            _tableBindingProvider = new CompositeArgumentBindingProvider(
                new StorageTableArgumentBindingProvider(),
                new CloudTableArgumentBindingProvider(),
                new QueryableArgumentBindingProvider(),
                new CollectorArgumentBindingProvider(),
                new AsyncCollectorArgumentBindingProvider(),
                new TableArgumentBindingExtensionProvider(extensions));

            _entityBindingProvider =
                new CompositeEntityArgumentBindingProvider(
                new TableEntityArgumentBindingProvider(),
                new PocoEntityArgumentBindingProvider()); // Supports all types; must come after other providers
        }

        public static IBindingProvider Build(INameResolver nameResolver, IConverterManager converterManager, IStorageAccountProvider accountProvider, IExtensionRegistry extensions)
        {
            var original = new TableAttributeBindingProvider(nameResolver, accountProvider, extensions);

            converterManager.AddConverter<JObject, ITableEntity, TableAttribute>(original.JObjectToTableEntityConverterFunc);

            var bindingFactory = new BindingFactory(nameResolver, converterManager);
            var bindAsyncCollector = bindingFactory.BindToAsyncCollector<TableAttribute, ITableEntity>(original.BuildFromTableAttribute);

            var bindToJobject = bindingFactory.BindToExactAsyncType<TableAttribute, JObject>(original.BuildJObject);

            // Filter to just support JObject, and use legacy bindings for everything else. 
            // Once we have ITableEntity converters for pocos, we can remove the filter. 
            // https://github.com/Azure/azure-webjobs-sdk/issues/887
            bindAsyncCollector = bindingFactory.AddFilter<TableAttribute>(
                (attr, type) => (type == typeof(IAsyncCollector<JObject>) || type == typeof(ICollector<JObject>)), 
                bindAsyncCollector); 

            var bindingProvider = new GenericCompositeBindingProvider<TableAttribute>(
                new IBindingProvider[] { bindToJobject, bindAsyncCollector, original });

            return bindingProvider;
        }

        // attr is already resolved.
        private async Task<JObject> BuildJObject(TableAttribute attribute)
        {
            IStorageTable table = GetTable(attribute);

            IStorageTableOperation retrieve = table.CreateRetrieveOperation<DynamicTableEntity>(
              attribute.PartitionKey, attribute.RowKey);
            TableResult result = await table.ExecuteAsync(retrieve, CancellationToken.None);
            DynamicTableEntity entity = (DynamicTableEntity)result.Result;
            if (entity == null)
            {
                return null;
            }
            else
            {
                var obj = ConvertEntityToJObject(entity);
                return obj;
            }
        }

        private IAsyncCollector<ITableEntity> BuildFromTableAttribute(TableAttribute attribute)
        {
            IStorageTable table = GetTable(attribute);

            var writer = new TableEntityWriter<ITableEntity>(table);
            return writer;
        }

        // Get the storage table from the attribute.
        private IStorageTable GetTable(TableAttribute attribute)
        {
            // $$$ multi account
            var account = _accountProvider.GetStorageAccountAsync(CancellationToken.None).GetAwaiter().GetResult();
            var tableClient = account.CreateTableClient();
            IStorageTable table = tableClient.GetTableReference(attribute.TableName);
            return table;
        }

        private ITableEntity JObjectToTableEntityConverterFunc(JObject source, TableAttribute attribute)
        {
            var result = this.CreateTableEntityFromJObject(attribute.PartitionKey, attribute.RowKey, source);
            return result;
        }

        public async Task<IBinding> TryCreateAsync(BindingProviderContext context)
        {
            ParameterInfo parameter = context.Parameter;
            TableAttribute tableAttribute = parameter.GetCustomAttribute<TableAttribute>(inherit: false);

            if (tableAttribute == null)
            {
                return null;
            }

            string tableName = Resolve(tableAttribute.TableName);
            IStorageAccount account = await _accountProvider.GetStorageAccountAsync(context.Parameter, context.CancellationToken, _nameResolver);
            StorageClientFactoryContext clientFactoryContext = new StorageClientFactoryContext
            {
                Parameter = context.Parameter
            };
            IStorageTableClient client = account.CreateTableClient(clientFactoryContext);

            bool bindsToEntireTable = tableAttribute.RowKey == null;
            IBinding binding;

            if (bindsToEntireTable)
            {
                IBindableTablePath path = BindableTablePath.Create(tableName);
                path.ValidateContractCompatibility(context.BindingDataContract);

                IStorageTableArgumentBinding argumentBinding = _tableBindingProvider.TryCreate(parameter);

                if (argumentBinding == null)
                {
                    throw new InvalidOperationException("Can't bind Table to type '" + parameter.ParameterType + "'.");
                }

                binding = new TableBinding(parameter.Name, argumentBinding, client, path);
            }
            else
            {
                string partitionKey = Resolve(tableAttribute.PartitionKey);
                string rowKey = Resolve(tableAttribute.RowKey);
                IBindableTableEntityPath path = BindableTableEntityPath.Create(tableName, partitionKey, rowKey);
                path.ValidateContractCompatibility(context.BindingDataContract);

                IArgumentBinding<TableEntityContext> argumentBinding = _entityBindingProvider.TryCreate(parameter);

                if (argumentBinding == null)
                {
                    throw new InvalidOperationException("Can't bind Table entity to type '" + parameter.ParameterType + "'.");
                }

                binding = new TableEntityBinding(parameter.Name, argumentBinding, client, path);
            }

            return binding;
        }

        private string Resolve(string queueName)
        {
            if (_nameResolver == null)
            {
                return queueName;
            }

            return _nameResolver.ResolveWholeString(queueName);
        }

        private static JObject ConvertEntityToJObject(DynamicTableEntity tableEntity)
        {
            JObject jsonObject = new JObject();
            foreach (var entityProperty in tableEntity.Properties)
            {
                JValue value = null;
                switch (entityProperty.Value.PropertyType)
                {
                    case EdmType.String:
                        value = new JValue(entityProperty.Value.StringValue);
                        break;
                    case EdmType.Int32:
                        value = new JValue(entityProperty.Value.Int32Value);
                        break;
                    case EdmType.Int64:
                        value = new JValue(entityProperty.Value.Int64Value);
                        break;
                    case EdmType.DateTime:
                        value = new JValue(entityProperty.Value.DateTime);
                        break;
                    case EdmType.Boolean:
                        value = new JValue(entityProperty.Value.BooleanValue);
                        break;
                    case EdmType.Guid:
                        value = new JValue(entityProperty.Value.GuidValue);
                        break;
                    case EdmType.Double:
                        value = new JValue(entityProperty.Value.DoubleValue);
                        break;
                    case EdmType.Binary:
                        value = new JValue(entityProperty.Value.BinaryValue);
                        break;
                }

                jsonObject.Add(entityProperty.Key, value);
            }

            jsonObject.Add("PartitionKey", tableEntity.PartitionKey);
            jsonObject.Add("RowKey", tableEntity.RowKey);

            return jsonObject;
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
