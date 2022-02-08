from pydantic import create_model

DynamicModel = create_model(
    'DynamicModel',
)

args = DynamicModel(table='billing_account', name="ABC")

print(args.dict())
