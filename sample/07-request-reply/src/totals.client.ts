import { Injectable } from '@nestjs/common';
import { KafkaRequestReplyService } from '@nest-native/kafka';
import { TotalQuery, TotalResult, TotalsConsumer } from './totals.consumer';

/**
 * The requesting side: the migration target for `ClientKafka.send()`.
 *
 * There is no `subscribeToResponseOf` and no per-pattern reply topic. One
 * shared reply topic is configured on the module, and this instance's replies
 * find it through its own ephemeral consumer group — see
 * `website/docs/request-reply.md` for why, and for what the fan-out costs.
 */
@Injectable()
export class TotalsClient {
  constructor(private readonly requests: KafkaRequestReplyService) {}

  /**
   * Ask for a total and wait for the answer.
   *
   * `value` is serialized explicitly, exactly as for every other produce in
   * this package. The reply comes back deserialized like any consumed payload.
   */
  async total(customerId: string, timeoutMs?: number): Promise<TotalResult> {
    const query: TotalQuery = { customerId };
    const reply = await this.requests.request<TotalResult>(
      {
        topic: TotalsConsumer.requestTopic,
        message: { key: customerId, value: JSON.stringify(query) },
      },
      { timeoutMs },
    );
    return reply.value;
  }

  /**
   * The same call against a topic whose handler fails non-retryably, so the
   * caller gets `KafkaReplyRemoteError` instead of waiting out the timeout.
   */
  async totalExpectingRejection(customerId: string): Promise<TotalResult> {
    const reply = await this.requests.request<TotalResult>({
      topic: TotalsConsumer.rejectTopic,
      message: { key: customerId, value: JSON.stringify({ customerId }) },
    });
    return reply.value;
  }

  /**
   * A request nobody answers, to show what a timeout means: **unknown**, not
   * "it did not happen". The request is on the topic and may still be processed
   * after this rejects, which is why the transport never retries for you.
   */
  async totalFromNobody(
    customerId: string,
    timeoutMs: number,
  ): Promise<TotalResult> {
    const reply = await this.requests.request<TotalResult>(
      {
        topic: 'orders.total.unanswered',
        message: { key: customerId, value: JSON.stringify({ customerId }) },
      },
      { timeoutMs },
    );
    return reply.value;
  }
}
