using Amazon.SecretsManager;
using Amazon.SecretsManager.Model;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Serilog;

namespace SecretManager.Impl
{
    /// <summary>
    /// Internal provider for AWS secrets manager
    /// </summary>
    public class AWSSecretsManagerProvider : IAWSSecretsManagerProvider
    {
        //private read-only variables
        private readonly IConfiguration _configuration;


        private static Dictionary<string, string> _secretValuesConfig;

        public Dictionary<string, string> secretsValueConfig { get; set; }

        /// <summary>
        /// Constructor for the AWSSecretsManagerProvider
        /// </summary>
        /// <param name="configuration"></param>
        /// <param name="log"></param>
        public AWSSecretsManagerProvider(IConfiguration configuration)
        {
            _configuration = configuration;
            secretsValueConfig = _secretValuesConfig;
        }

        /// <summary>
        /// returns the GetSecretValueResponse response by sending the GetSecretValueRequest request using the client and secret name
        /// </summary>
        /// <param name="secretName"></param>
        /// <returns></returns>
        public async Task<string> GetSecretAsync(string secName)
        {
            var config = new AmazonSecretsManagerConfig();
            var client = new AmazonSecretsManagerClient(config);

            var request = new GetSecretValueRequest
            {
                SecretId = secName
            };
            Log.Logger.Information($"Getting the secrets from AWS Secrets Manager for the secret {secName}");
            GetSecretValueResponse response = null;
            try
            {
                response = await client.GetSecretValueAsync(request).ConfigureAwait(false);
            }
            catch (ResourceNotFoundException exception)
            {
                Log.Logger.Error($"The requested secret {secName} was not found");
                Log.Logger.Error($"{secName},{exception}");

            }
            catch (InvalidRequestException exception)
            {
                Log.Logger.Error($"The request was invalid due to: {exception.Message}");
                Log.Logger.Error($"{secName},{exception}");
            }
            catch (InvalidParameterException exception)
            {
                Log.Logger.Error($"The request had invalid params: {exception.Message}");
                Log.Logger.Error($"{secName},{exception}");
            }
            catch (Exception exception)
            {
                Log.Logger.Error($"Exception : {secName}, {exception.Message}");
            }

            return response?.SecretString;
        }

        public void GetSecretValues(string secretString)
        {
            try
            {
                _secretValuesConfig = JsonConvert.DeserializeObject<Dictionary<string, string>>(secretString);
                if (_secretValuesConfig != null && _secretValuesConfig.Count > 0)
                {
                    foreach (string secretsKey in _secretValuesConfig.Keys)
                    {
                        if (string.IsNullOrEmpty(_secretValuesConfig[secretsKey]))
                        {
                            throw new Exception("Expected Value for the Key from AWS Secrets Manager but found empty or Null for Key: " + secretsKey);
                        }
                    }
                }
                else
                {
                    throw new Exception("Exception while deserializing the secrets from AWS");
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Exception while getting the secrets" + ex.Message);
            }
        }
    }
}
