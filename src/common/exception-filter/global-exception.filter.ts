import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
  HttpStatus, Inject,
} from '@nestjs/common';
import { format } from 'date-fns';

import { ErrorResponse } from '@/common/interceptors/response-error.interceptor';
import { WinstonLoggerService } from '@/logger/winston-logger.service';

@Catch()
export class GlobalExceptionFilter implements ExceptionFilter {
  constructor(@Inject(WinstonLoggerService) private readonly winstonLoggerService: WinstonLoggerService) {
  }

  catch(exception: any, host: ArgumentsHost) {
    // console.log(exception);
    const ctx = host.switchToHttp();
    const response = ctx.getResponse();
    const request = ctx.getRequest();

    const statusCode =
      exception instanceof HttpException
        ? exception.getStatus()
        : HttpStatus.INTERNAL_SERVER_ERROR;
    const exceptionResponse = exception.getResponse();
    const errorMessage =
      typeof exceptionResponse === 'object' && 'message' in exceptionResponse
        ? exceptionResponse['message']
        : exception.message;

    const finalRes: ErrorResponse = {
      status: false,
      path: request.url,
      statusCode,
      error: errorMessage,
      timestamp: format(new Date().toISOString(), 'yyyy-MM-dd HH:mm:ss'),
    };
    this.winstonLoggerService.error(`GlobalExceptionFilter - `, JSON.stringify(finalRes));

    return response.code(statusCode).send(finalRes);
  }
}
