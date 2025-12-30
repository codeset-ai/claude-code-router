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

function flattenContentToString(content: any): string {
  if (typeof content === 'string') {
    return content;
  }
  if (Array.isArray(content)) {
    return content.filter((block: any) => block.type === 'text' && block.text)
        .map((block: any) => block.text)
        .join('\n');
  }
  return '';
}

export class MistralTransformer {
  static TransformerName = 'mistral';

  async transformRequestIn(request: any): Promise<any> {
    if (request.system) {
      request.system = flattenContentToString(request.system);
    }

    if (!request.messages) {
      return request;
    }

    for (const message of request.messages) {
      if (message.role === 'system' && Array.isArray(message.content)) {
        message.content = flattenContentToString(message.content);
      }

      if (message.role === 'user' && Array.isArray(message.content)) {
        const hasOnlyText =
            message.content.every((block: any) => block.type === 'text');
        if (hasOnlyText) {
          message.content = flattenContentToString(message.content);
        }
      }

      if (message.role === 'assistant' && message.tool_calls) {
        for (const toolCall of message.tool_calls) {
          if (toolCall.id && !isValidMistralId(toolCall.id)) {
            toolCall.id = getOrCreateShortId(toolCall.id);
          }
        }
      }

      if (message.role === 'tool' && message.tool_call_id &&
          !isValidMistralId(message.tool_call_id)) {
        message.tool_call_id = getOrCreateShortId(message.tool_call_id);
      }

      if (Array.isArray(message.content)) {
        for (const block of message.content) {
          if (block.type === 'tool_use' && block.id &&
              !isValidMistralId(block.id)) {
            block.id = getOrCreateShortId(block.id);
          }
          if (block.type === 'tool_result' && block.tool_use_id &&
              !isValidMistralId(block.tool_use_id)) {
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

    const processLine = (line: string): string => {
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

          return 'data: ' + JSON.stringify(data);
        } catch {
          return line;
        }
      }
      return line;
    };

    if (contentType.includes('text/event-stream') ||
        contentType.includes('stream')) {
      if (!response.body) {
        return response;
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      const encoder = new TextEncoder();

      const stream = new ReadableStream({
        async start(controller) {
          let buffer = '';

          try {
            while (true) {
              const {done, value} = await reader.read();
              if (done) {
                if (buffer.trim()) {
                  controller.enqueue(encoder.encode(buffer));
                }
                break;
              }

              if (!value || value.length === 0) {
                continue;
              }

              let chunk;
              try {
                chunk = decoder.decode(value, {stream: true});
              } catch (decodeError) {
                console.warn('Failed to decode chunk', decodeError);
                continue;
              }

              if (chunk.length === 0) {
                continue;
              }

              buffer += chunk;

              if (buffer.length > 1000000) {
                console.warn(
                    'Buffer size exceeds limit, processing partial data');
                const lines = buffer.split('\n');
                buffer = lines.pop() || '';

                for (const line of lines) {
                  if (line.trim()) {
                    controller.enqueue(
                        encoder.encode(processLine(line) + '\n'));
                  }
                }
                continue;
              }

              const lines = buffer.split('\n');
              buffer = lines.pop() || '';

              for (const line of lines) {
                if (!line.trim()) continue;
                controller.enqueue(encoder.encode(processLine(line) + '\n'));
              }
            }
          } catch (error) {
            console.error('Stream error:', error);
            controller.error(error);
          } finally {
            try {
              reader.releaseLock();
            } catch (e) {
              console.error('Error releasing reader lock:', e);
            }
            controller.close();
          }
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
