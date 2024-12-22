/*
 * Copyright (C) 2018 Square, Inc.
 * Copyright (C) 2024 Uli Bubenheimer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

@file:JvmName("FlowQuery")

package app.cash.sqldelight.coroutines

import app.cash.sqldelight.Query
import app.cash.sqldelight.async.coroutines.awaitAsList
import app.cash.sqldelight.async.coroutines.awaitAsOne
import app.cash.sqldelight.async.coroutines.awaitAsOneOrNull
import kotlin.coroutines.CoroutineContext
import kotlin.jvm.JvmName
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.conflate
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.shareIn
import kotlinx.coroutines.withContext

/**
 * Turns this [Query] into a [Flow] which emits whenever the underlying result set changes.
 *
 * This flow operator is modified from its original implementation:
 * 1. Conflated buffer is subject to operator/buffer fusion; conflation overrides any other fused
 * buffer specification. This facilitates usage with [shareIn].
 * 2. Suspends initially as an unfortunate consequence of using [callbackFlow].
 */
@JvmName("toFlow")
fun <T : Any> Query<T>.asFlow(): Flow<Query<T>> = callbackFlow {
  channel.send(this@asFlow)

  val listener = Query.Listener {
    channel.trySend(this@asFlow).getOrThrow() // should never throw
  }

  addListener(listener)

  awaitClose { removeListener(listener) }
}.conflate()

fun <T : Any> Flow<Query<T>>.mapToOne(
  context: CoroutineContext,
): Flow<T> = map {
  withContext(context) {
    it.awaitAsOne()
  }
}

fun <T : Any> Flow<Query<T>>.mapToOneOrDefault(
  defaultValue: T,
  context: CoroutineContext,
): Flow<T> = map {
  withContext(context) {
    it.awaitAsOneOrNull() ?: defaultValue
  }
}

fun <T : Any> Flow<Query<T>>.mapToOneOrNull(
  context: CoroutineContext,
): Flow<T?> = map {
  withContext(context) {
    it.awaitAsOneOrNull()
  }
}

fun <T : Any> Flow<Query<T>>.mapToOneNotNull(
  context: CoroutineContext,
): Flow<T> = mapNotNull {
  withContext(context) {
    it.awaitAsOneOrNull()
  }
}

fun <T : Any> Flow<Query<T>>.mapToList(
  context: CoroutineContext,
): Flow<List<T>> = map {
  withContext(context) {
    it.awaitAsList()
  }
}
