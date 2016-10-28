﻿// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;

namespace Microsoft.Azure.WebJobs
{
    /// <summary>
    /// Attribute used to bind a parameter to an Azure Table or Table entity.
    /// </summary>
    /// <remarks>
    /// When only the table name is provided, the attribute binds to a table, and the method parameter type can be one
    /// of the following:
    /// <list type="bullet">
    /// <item><description>CloudTable</description></item>
    /// <item><description><see cref="IQueryable{T}"/> (where T implements ITableEntity)</description></item>
    /// </list>
    /// When the table name, partition key, and row key are provided, the attribute binds to a table entity, and the
    /// method parameter type can be one of the following:
    /// <list type="bullet">
    /// <item><description>ITableEntity</description></item>
    /// <item><description>
    /// A user-defined type not implementing ITableEntity (serialized as strings for simple types and JSON for complex
    /// types)
    /// </description></item>
    /// </list>
    /// </remarks>
    [AttributeUsage(AttributeTargets.Parameter)]
    [DebuggerDisplay("{DebuggerDisplay,nq}")]
    public sealed class TableAttribute : Attribute
    {
        private readonly string _tableName;
        private readonly string _partitionKey;
        private readonly string _rowKey;

        /// <summary>Initializes a new instance of the <see cref="TableAttribute"/> class.</summary>
        /// <param name="tableName">The name of the table to which to bind.</param>
        public TableAttribute(string tableName)
        {
            _tableName = tableName;
        }

        /// <summary>Initializes a new instance of the <see cref="TableAttribute"/> class.</summary>
        /// <param name="tableName">The name of the table containing the entity.</param>
        /// <param name="partitionKey">The partition key of the entity.</param>
        public TableAttribute(string tableName, string partitionKey)
        {
            _tableName = tableName;
            _partitionKey = partitionKey;
        }

        /// <summary>Initializes a new instance of the <see cref="TableAttribute"/> class.</summary>
        /// <param name="tableName">The name of the table containing the entity.</param>
        /// <param name="partitionKey">The partition key of the entity.</param>
        /// <param name="rowKey">The row key of the entity.</param>
        public TableAttribute(string tableName, string partitionKey, string rowKey)
        {
            _tableName = tableName;
            _partitionKey = partitionKey;
            _rowKey = rowKey;
        }

        /// <summary>Gets the name of the table to which to bind.</summary>
        /// <remarks>When binding to a table entity, gets the name of the table containing the entity.</remarks>
        [AutoResolve]
        public string TableName
        {
            get { return _tableName; }
        }

        /// <summary>When binding to a table entity, gets the partition key of the entity.</summary>
        /// <remarks>When binding to an entire table, returns <see langword="null"/>.</remarks>
        [AutoResolve]
        public string PartitionKey
        {
            get { return _partitionKey; }
        }

        /// <summary>When binding to a table entity, gets the row key of the entity.</summary>
        /// <remarks>When binding to an entire table, returns <see langword="null"/>.</remarks>
        [AutoResolve]
        public string RowKey
        {
            get { return _rowKey; }
        }

        /// <summary>
        /// Allow arbitrary table filter. RowKey should be null. 
        /// </summary>
        [AutoResolve]
        public string Filter
        {
            get; set;
        }

        /// <summary>
        /// Used with filter. RowKey should be null. 
        /// </summary>
        public int Take
        {
            get; set;
        }        

        [SuppressMessage("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode")]
        private string DebuggerDisplay
        {
            get
            {
                if (_rowKey == null)
                {
                    return _tableName;
                }
                else
                {
                    return String.Format(CultureInfo.InvariantCulture, "{0}(PK={1}, RK={2})",
                        _tableName, _partitionKey, _rowKey);
                }
            }
        }
    }
}
