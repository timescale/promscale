package generator

import (
	"testing"

	"github.com/dgrijalva/jwt-go/v4"
	"github.com/stretchr/testify/require"
)

func TestGenerateToken(t *testing.T) {
	// All testing tokens are generated from https://cryptotools.net/rsagen
	tcs := []struct{
		name string
		config *Config
		publicKey string
	}{
		{
			name: "token 1024",
			config: &Config{
				PrivateKey: `-----BEGIN RSA PRIVATE KEY-----
MIICWwIBAAKBgEuaLYiKrr/EwlzRiCF7UvyI3zTDwUwmd0MnibSXUb+3QcTtOmTd
Dedwq/nl5VrlqmnW9P7Vh3EIeRz6RotGVNYq3BVf/L1scEdv67TcZaE5f8OpVEzt
ImNVk0VfWMWP67pAQKXo+rR3aT9DRrYRosb3p5XhZw+t0sgbyoNcgQ3PAgMBAAEC
gYAoMgTbf8CBPP+Jke2qv4LTuYHS7/Epp5npHnBcj8drAuf455gQYGcwEfudldln
howgUaVYh/bG5hZejcJL8kzQLUBJlrqmUut+7BYIfmKqZe5SwBgxF2K2nlYYbgwo
9D5+WdKSWQjROZW+eGrFqZtrzVl/2vmHjXilViqRCKiPwQJBAJJ5UQkPguNdubz4
EY+kfo+Vp71qdEsxLPyVAEky4syTge4nM8nFNl8w7ZGOPeFnol234JzageZ+t0KY
JIF2KZkCQQCEIkxEUn4OzfK1h+WRJZQJ65LO4sFFqjmpKKgYFVF3FiLsf3NjdE7K
nQ+sL72ZsnLMJq812ES6IbQMp/VUHCOnAkB28zqR5xzeVDEQe0yXoHh/VoPAWYFT
xv3HqaFv0HlKtcfghcmS5CtBptRnTmKGeMjs3vTzrKetbd6ZoECVOkaRAkAhuoOP
WSNWchnHXtYp09bTJXAHIXjGaKppVLh71U1DQcJObkYtF+5Y5/itMk6KojONiz5k
Vz1L6fiE1obYHoq3AkEAis1plRylcpUKub2+jm6oHcrqDsnUmPDHXOJZvRu7MbtN
XKYt0bMwAKnH45oatS8awsXE6oV+Xlje/InfVZDI3Q==
-----END RSA PRIVATE KEY-----`,
				Payload: Payload{
					Issuer: "user",
					Audience: "public",
					TenantList: []string{"tenant-x"},
				},
			},
			publicKey: `-----BEGIN PUBLIC KEY-----
MIGeMA0GCSqGSIb3DQEBAQUAA4GMADCBiAKBgEuaLYiKrr/EwlzRiCF7UvyI3zTD
wUwmd0MnibSXUb+3QcTtOmTdDedwq/nl5VrlqmnW9P7Vh3EIeRz6RotGVNYq3BVf
/L1scEdv67TcZaE5f8OpVEztImNVk0VfWMWP67pAQKXo+rR3aT9DRrYRosb3p5Xh
Zw+t0sgbyoNcgQ3PAgMBAAE=
-----END PUBLIC KEY-----`,
		}, {
			name: "token 2048",
			config: &Config{
				PrivateKey: `-----BEGIN RSA PRIVATE KEY-----
MIIEogIBAAKCAQEAqCLa5bBjJyIE+mi4FhtIe8xfXTCAUUShFuBQNMgF3J+gKXQh
G/pWu8GMHRDmMdDCOeBkEWmD1oZk5OFYCUe32ETQra0sqPFeCvAaPXUh8ih1FpwV
MmjEBjokDPaf+kdAsL8Bv5No4gRzLP581GSdL3/5Oyed6qe9z/XuoFBTIFmWYyrQ
LIhVhh4WNEsieYUa4398cg3Sc1RE3lgoYubz7IS+7KbWbBAcyEDi+FRH4++cb71H
rjA5tRu6Z3pHhQnJEToEbHdi26T3JnW7nnSKPHTwyRZU+fxshnuOS6OkBgYwktPC
GxVvS2um7UJlSgfSoPMklfgsADTW/wmPqGGH5wIDAQABAoIBADkhptnXZsm8UI66
GJtCaA3Q8zKaMW/r7wTz8a0Nrpg6EVUkpb95ABHKgY662E1jDyxVaAtWnDq/7far
75svqHOvZm0tY/iAE6GJ1pC9hWxgfPDS1o0LdlhbVzakctW7cmrcbH6pW0E5m91V
GBufmJ20UX7dRlae6uuIOK52vNhHq4Zlw+iBvQOcxOZ4tq3b5R9/E9Secu6mZeti
Ez5+ccpr2NBR4oITqqZMbpCmGRZQ4P5qutUBSJvkpTpS+Ku6/2/pMfEeYSNjIZpH
x+oB4QQgIfg8rhGNxdCiuH8GphiXq6zaPjZfSl/y/hynVxrxBv9QptN9COrPS3Un
WeILvTkCgYEA3G86+L1Y6wEEZksPGQUhoMihCGtaGEe7x2caJSv4gSfxR9Snv9j0
940SDYSnNnAm6Ge7/uEPx/Clrk9jSp0Wd2gtfGyxkTQ636JRa8kldlM0L3wuiZHl
o1HQzOV89q0oQu9tLkKGr4Eg6MszyZmleX5VT0DrMHJFRnQETVexxM0CgYEAw0OB
6kjIJLC9qzrBdJm1dxmdFZd3KTf2GvjnR/EBjRJiP71SLos3yNzhx20R4L1kWlN9
WB3iJzEvfBBFNmxITjZwpl4OvtClkj2ZZPQV5buCawI4pBmKPtxN2zumQZI7z7he
trGyLqFFTfMXbZ9A/+OuWeZzgk9cmWayLa/pH4MCgYBvYRxw9mtpQrhQX3ifccJO
FVGYiXWacxRkaqoBQjhsOhxl6QNhomQUeQk4H2DF1uEx6MhKvrlUkzmD28UVvE3P
w6nIBrup3LQqrz9osNfm85+Ypqx1Amz/mqVgAkyj71Y6i/OkviSDpUFVnubp5GKC
At1kEC9F6VcrhA9wbPD7uQKBgFli4OkQMlbhdaIZ/o9TAggoeIl20fzTelz0jmOX
hz8+KC3HjAR0hK/1tYGmg2WfLX5P8/RPkcShNnyTsitsvFjZgQ4XxqZBO1pLypm1
RwToppY36Rft/SQzk4yFrOEdgTXgz7LQe6Hu/5vkaVgsbAP4BUtwPHZtwBUVNwHF
InHjAoGAL4Pd+1/QgjQw5lrH2AknHrci5sM5NlaJNlToBmdBVJDls5jBvSM83LmC
THLEggTOWxD1rYyq3qcVb9nSEoZFJFnj9yVyJHz70B9cryc0Sle6ua7PPr5sKRGr
P4cLp7tBo0/PXyqplPibaqDfz5f76bwJGdRL3157pvf3Fq0RoQo=
-----END RSA PRIVATE KEY-----`,
				Payload: Payload{
					Issuer: "user",
					Audience: "public",
					TenantList: []string{"tenant-x"},
				},
			},
			publicKey: `-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAqCLa5bBjJyIE+mi4FhtI
e8xfXTCAUUShFuBQNMgF3J+gKXQhG/pWu8GMHRDmMdDCOeBkEWmD1oZk5OFYCUe3
2ETQra0sqPFeCvAaPXUh8ih1FpwVMmjEBjokDPaf+kdAsL8Bv5No4gRzLP581GSd
L3/5Oyed6qe9z/XuoFBTIFmWYyrQLIhVhh4WNEsieYUa4398cg3Sc1RE3lgoYubz
7IS+7KbWbBAcyEDi+FRH4++cb71HrjA5tRu6Z3pHhQnJEToEbHdi26T3JnW7nnSK
PHTwyRZU+fxshnuOS6OkBgYwktPCGxVvS2um7UJlSgfSoPMklfgsADTW/wmPqGGH
5wIDAQAB
-----END PUBLIC KEY-----`,
		},{
			name: "token 4096",
			config: &Config{
				PrivateKey: `-----BEGIN RSA PRIVATE KEY-----
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
-----END RSA PRIVATE KEY-----`,
				Payload: Payload{
					Issuer: "user",
					Audience: "public",
					TenantList: []string{"tenant-x"},
				},
			},
			publicKey: `-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAgKDmdCZp28giOCOG4LLG
5+s0eAx9xfewXhemstKwzwBZtLzjmQQt+pjcvMzYwTm+sqeliziEsxh6Evw1hdyK
W6NSXHYCkUq1ThPepXn1dM++j72rvvdWiC31ptOdWP6KuQCZbuAjZsN2KAHGInJM
0FXPkIrmHhuP4S3QDq6a+citNDgSoMZm/RfxHtFcoqm0jhKEodSHK3BaHrbiq/au
TyGhF3eHzevnotenKMzJbVjZWD54eNtGy5s6MSaV/cscsMZjJhpHljFZc0nsm4l3
bGvSNaO7twJ/ilQkcv9VEQyT7byMwe568Ri37UvBP+qG1LxyoI9VklcsrDLKXrsS
AjMV+JO+BN2SFldXcla9htE0H4v52eLFCpOkrw5KJdHv4kQrawDIP4yV+EcVUPrs
kDyN69w94xp7aFv8jKREHfJ61M324X8uts3WiFvW7fauA+D12341FbQB9R+p0a8S
X4LFMLkX9LMdrr0lUpVx2n9+QiGd2P66G0nvoe9sve+wf3mT9TWiye7B287wAGKO
o8BZTVaMR2mT2GQ3RnCbEPjQB6D2jjL2G5EmYUBdNbJn/uzk/HQXWcCld9uHt4w7
b0M48RZqrPwR40iaFoAI1Xy6WYKZDuWlYE8y2EAQfKGn4zcnGJNKl+XA6rXHYF+B
fHhRZEeCwKxdAKOBuIjpECkCAwEAAQ==
-----END PUBLIC KEY-----`,
		}, {
			name: "token 512",
			config: &Config{
				// From https://csfieldguide.org.nz/en/interactives/rsa-key-generator/ choose key size as 512 bits, format scheme PKCS #8.
				PrivateKey: `-----BEGIN PRIVATE KEY-----
MIIBVAIBADANBgkqhkiG9w0BAQEFAASCAT4wggE6AgEAAkEAkGvfwrjaBen4ayXJ
houwPDk8z3HkZu2Llil6MU4hGFv0EIlRMYaDo0e/oABaglnyMNbBrQhiEJpNgDUb
7V6arwIDAQABAkBXNSybErBkndHqMZkta7BxzUZRJb78ADkn5cfpefo2EHfEzx2x
/hT9x+JVnp2OckW2bVT7fN1HJKptcGbAHlChAiEAzzEXtFvKeq2a41qjBeXZtuPU
Xk1tbplBoRHAEMJ3UVcCIQCycVm2mYyjNHGXeJRBum1XaPysajca7SiCStwoFIby
aQIgLCccFBVTit0gGr8f6ovW34Chqw74+Q6zy61KrseiQbUCIHEEYbHJJs3nVVp+
QEgw7zfBgucgjq47LsD28OFhvuahAiEAmn5KEaXLamy2W6LG1AiV6jlT2PdbK3Zs
r7d3tUJUi9o=
-----END PRIVATE KEY-----`,
				Payload: Payload{
					Issuer: "user",
					Audience: "public",
					TenantList: []string{"tenant-x"},
				},
			},
			publicKey: `-----BEGIN PUBLIC KEY-----
MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAJBr38K42gXp+GslyYaLsDw5PM9x5Gbt
i5YpejFOIRhb9BCJUTGGg6NHv6AAWoJZ8jDWwa0IYhCaTYA1G+1emq8CAwEAAQ==
-----END PUBLIC KEY-----`,
		},
	}
	for _, tc := range tcs {
		token, err := GenerateToken(tc.config)
		require.NoErrorf(t, err, tc.name)

		publicKey, err := jwt.ParseRSAPublicKeyFromPEM([]byte(tc.publicKey))
		require.NoError(t, err)

		claims := Claims{}
		_, err = jwt.ParseWithClaims(token, &claims, func(token *jwt.Token) (interface{}, error) {
			token.Claims.Valid(jwt.NewValidationHelper(jwt.WithoutAudienceValidation()))
			return publicKey, nil
		}, jwt.WithAudience(tc.config.Audience))

		require.NoError(t, err)

		// Verify claims.
		if claims.Issuer != tc.config.Issuer {
			require.Fail(t, "issuer from parsed claims do not match with actual")
		}

		if claims.Audience[0] != tc.config.Audience {
			require.Fail(t, "issuer from parsed claims do not match with actual")
		}

		if claims.TenantList[0] != tc.config.TenantList[0] {
			require.Fail(t, "tenant list from parsed claims do not match with actual")
		}
	}
}
