import {
  CallHandler,
  ExecutionContext,
  HttpException,
  HttpStatus,
  Injectable,
  NestInterceptor,
} from '@nestjs/common';
import { format } from 'date-fns';
import { catchError, Observable, throwError, timeout } from 'rxjs';

export type ErrorResponse = {
  status: boolean;
  statusCode: number;
  path: string;
  error: any;
  timestamp: string;
};

@Injectable()
export class ResponseErrorInterceptor implements NestInterceptor {
  intercept(
    context: ExecutionContext,
    next: CallHandler,
  ): Observable<ErrorResponse> {
    timeout(10000); // Maximum 10 sec is allowed for a request to handle.
    return next
      .handle()
      .pipe(
        catchError((err: HttpException) =>
          throwError(() => this.errorHandler(err, context)),
        ),
      );
  }

  errorHandler(exception: HttpException, context: ExecutionContext) {
    const ctx = context.switchToHttp();
    const request = ctx.getRequest();
    const response = ctx.getResponse();
    const statusCode =
      exception instanceof HttpException
        ? exception.getStatus()
        : HttpStatus.INTERNAL_SERVER_ERROR;
    const finalRes: ErrorResponse = {
      status: false,
      path: request.url,
      statusCode,
      error: exception.message,
      timestamp: format(new Date().toISOString(), 'yyyy-MM-dd HH:mm:ss'),
    };
    return response.code(statusCode).send(finalRes);
  }
}
