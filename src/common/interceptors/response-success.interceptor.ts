import {
  CallHandler,
  ExecutionContext,
  Injectable,
  NestInterceptor,
} from '@nestjs/common';
import { format } from 'date-fns';
import { map, Observable } from 'rxjs';

export type Response = {
  status: boolean;
  statusCode: number;
  path: string;
  data: any;
  timestamp: string;
};

@Injectable()
export class ResponseSuccessInterceptor implements NestInterceptor {
  intercept(
    context: ExecutionContext,
    next: CallHandler,
  ): Observable<Response> {
    return next.handle().pipe(
      map((data): Response => {
        return this.responseHandler(data, context);
      }),
    );
  }

  responseHandler(res: any, context: ExecutionContext) {
    const ctx = context.switchToHttp();
    const response = ctx.getResponse();
    const request = ctx.getRequest();
    const statusCode = response.statusCode;
    const finalRes: Response = {
      status: true,
      path: request.url,
      statusCode,
      data: res,
      timestamp: format(new Date().toISOString(), 'yyyy-MM-dd HH:mm:ss'),
    };
    return finalRes;
  }
}
