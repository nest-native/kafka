import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { OrdersService } from './orders.service';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.createApplicationContext(AppModule);
  app.enableShutdownHooks();

  const orders = app.get(OrdersService);
  await orders.placeOrder({ id: 'order-1', total: 4200 });
  await orders.placeOrders([
    { id: 'order-2', total: 1300 },
    { id: 'order-3', total: 9001 },
  ]);

  await app.close();
}

void bootstrap();
