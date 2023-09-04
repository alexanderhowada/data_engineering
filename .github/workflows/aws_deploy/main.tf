variable AWS_LAMBDA_ROLE {}

provider "aws" {}

# resource "aws_iam_role" "test_role" {
#     name="test_role"
#     assume_role_policy=jsonencode({
#         Version = "2017-10-17",
#         Statement = [
#             {
#                 Action = "sts:AssumeRole",
#                 Effect = "Allow",
#                 Principal = {
#                     Service = "lambda.amazonaws.com"
#                 }
#             }
#         ]
#     })
# }
#
# resource "aws_iam_policy_attachment" "attach_s3full_test_role" {
#     policy_arn="arn:aws:iam::aws:policy/AmazonS3FullAccess"
#     name="iam_role.test_role.name"
# }

resource "aws_lambda_layer_version" "data_engineering" {
    filename="lambda_layer.zip"
    compatible_runtimes=["python3.11"]
    description="Full deploy"
    layer_name="data_engineering"
}

resource "aws_lambda_function" "multiply" {
    filename="mock.zip"
    function_name="multiply"
    handler="aws.aws_lambda.multiply.main.main"
    runtime="python3.11"
    role=var.AWS_LAMBDA_ROLE
    layers=[aws_lambda_layer_version.data_engineering.arn]
    depends_on=[aws_lambda_layer_version.data_engineering]
}

resource "aws_lambda_function" "first_lambda" {
    filename="mock.zip"
    function_name="first_lambda"
    handler="aws.aws_lambda.first_lambda.main.main"
    runtime="python3.11"
    role=var.AWS_LAMBDA_ROLE
    layers=[aws_lambda_layer_version.data_engineering.arn]
    depends_on=[aws_lambda_layer_version.data_engineering]
}