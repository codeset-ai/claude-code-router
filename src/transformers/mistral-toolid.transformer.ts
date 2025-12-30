const idMappings = new Map<string, string>();

function generateShortId(): string {
  const chars =
    'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let result = '';
  for (let i = 0; i < 9; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}

function isValidMistralId(id: string): boolean {
  return typeof id === 'string' && id.length === 9 && /^[a-zA-Z0-9]+$/.test(id);
}

function getOrCreateShortId(originalId: string): string {
  if (isValidMistralId(originalId)) {
    return originalId;
  }

  for (const [shortId, origId] of idMappings.entries()) {
    if (origId === originalId) {
      return shortId;
    }
  }

  let shortId = generateShortId();
  while (idMappings.has(shortId)) {
    shortId = generateShortId();
  }

  idMappings.set(shortId, originalId);
  return shortId;
}

function getOriginalId(shortId: string): string {
  return idMappings.get(shortId) || shortId;
}

export class MistralToolIdTransformer {
  static TransformerName = 'mistral-toolid';

  async transformRequestIn(request: any): Promise<any> {
    if (!request.messages) {
      return request;
    }

    for (const message of request.messages) {
      if (message.role === 'assistant' && message.tool_calls) {
        for (const toolCall of message.tool_calls) {
          if (toolCall.id && !isValidMistralId(toolCall.id)) {
            toolCall.id = getOrCreateShortId(toolCall.id);
          }
        }
      }

      if (
        message.role === 'tool' &&
        message.tool_call_id &&
        !isValidMistralId(message.tool_call_id)
      ) {
        message.tool_call_id = getOrCreateShortId(message.tool_call_id);
      }

      if (Array.isArray(message.content)) {
        for (const block of message.content) {
          if (
            block.type === 'tool_use' &&
            block.id &&
            !isValidMistralId(block.id)
          ) {
            block.id = getOrCreateShortId(block.id);
          }
          if (
            block.type === 'tool_result' &&
            block.tool_use_id &&
            !isValidMistralId(block.tool_use_id)
          ) {
            block.tool_use_id = getOrCreateShortId(block.tool_use_id);
          }
        }
      }
    }

    return request;
  }

  async transformResponseOut(response: Response): Promise<Response> {
    const contentType = response.headers.get('Content-Type') || '';

    if (contentType.includes('application/json')) {
      const jsonResponse = await response.json();

      if (jsonResponse?.choices) {
        for (const choice of jsonResponse.choices) {
          if (choice.message?.tool_calls) {
            for (const toolCall of choice.message.tool_calls) {
              if (toolCall.id) {
                const originalId = getOriginalId(toolCall.id);
                if (originalId !== toolCall.id) {
                  toolCall.id = originalId;
                }
              }
            }
          }
        }
      }

      return new Response(JSON.stringify(jsonResponse), {
        status: response.status,
        statusText: response.statusText,
        headers: response.headers,
      });
    }

    if (
      contentType.includes('text/event-stream') ||
      contentType.includes('stream')
    ) {
      const reader = response.body!.getReader();
      const decoder = new TextDecoder();
      const encoder = new TextEncoder();

      const stream = new ReadableStream({
        async start(controller) {
          let buffer = '';

          while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\n');
            buffer = lines.pop() || '';

            for (const line of lines) {
              let outputLine = line;

              if (line.startsWith('data: ') && line !== 'data: [DONE]') {
                try {
                  const data = JSON.parse(line.slice(6));

                  if (data.choices) {
                    for (const choice of data.choices) {
                      if (choice.delta?.tool_calls) {
                        for (const toolCall of choice.delta.tool_calls) {
                          if (toolCall.id) {
                            const originalId = getOriginalId(toolCall.id);
                            if (originalId !== toolCall.id) {
                              toolCall.id = originalId;
                            }
                          }
                        }
                      }
                    }
                  }

                  outputLine = 'data: ' + JSON.stringify(data);
                } catch {
                }
              }

              controller.enqueue(encoder.encode(outputLine + '\n'));
            }
          }

          if (buffer) {
            controller.enqueue(encoder.encode(buffer));
          }

          controller.close();
        },
      });

      return new Response(stream, {
        status: response.status,
        statusText: response.statusText,
        headers: response.headers,
      });
    }

    return response;
  }
}

