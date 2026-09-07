FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY . .
RUN dotnet restore src/OrderFulfillmentSaga/OrderFulfillmentSaga.Worker/OrderFulfillmentSaga.Worker.csproj
RUN dotnet publish src/OrderFulfillmentSaga/OrderFulfillmentSaga.Worker/OrderFulfillmentSaga.Worker.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS final
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "OrderFulfillmentSaga.Worker.dll"]
