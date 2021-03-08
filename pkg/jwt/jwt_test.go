package generator

import (
	"fmt"
	"testing"

	"github.com/dgrijalva/jwt-go/v4"
	"github.com/stretchr/testify/require"
	jwt_claims "github.com/timescale/promscale/pkg/jwt/claims"
	"github.com/timescale/promscale/pkg/jwt/generator"
	"github.com/timescale/promscale/pkg/jwt/verifier"
)

func TestGenerateToken(t *testing.T) {
	// All testing tokens are generated from https://cryptotools.net/rsagen
	tcs := []struct {
		name         string
		config       *generator.Config
		public       string
		isPublicFile bool
	}{
		{
			name: "token 1024",
			config: &generator.Config{
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
				Payload: generator.Payload{
					Issuer:   "user",
					Audience: "public",
				},
			},
			public: `-----BEGIN PUBLIC KEY-----
MIGeMA0GCSqGSIb3DQEBAQUAA4GMADCBiAKBgEuaLYiKrr/EwlzRiCF7UvyI3zTD
wUwmd0MnibSXUb+3QcTtOmTdDedwq/nl5VrlqmnW9P7Vh3EIeRz6RotGVNYq3BVf
/L1scEdv67TcZaE5f8OpVEztImNVk0VfWMWP67pAQKXo+rR3aT9DRrYRosb3p5Xh
Zw+t0sgbyoNcgQ3PAgMBAAE=
-----END PUBLIC KEY-----`,
		}, {
			name: "token 2048",
			config: &generator.Config{
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
				Payload: generator.Payload{
					Issuer:   "user",
					Audience: "public",
				},
			},
			public: `-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAqCLa5bBjJyIE+mi4FhtI
e8xfXTCAUUShFuBQNMgF3J+gKXQhG/pWu8GMHRDmMdDCOeBkEWmD1oZk5OFYCUe3
2ETQra0sqPFeCvAaPXUh8ih1FpwVMmjEBjokDPaf+kdAsL8Bv5No4gRzLP581GSd
L3/5Oyed6qe9z/XuoFBTIFmWYyrQLIhVhh4WNEsieYUa4398cg3Sc1RE3lgoYubz
7IS+7KbWbBAcyEDi+FRH4++cb71HrjA5tRu6Z3pHhQnJEToEbHdi26T3JnW7nnSK
PHTwyRZU+fxshnuOS6OkBgYwktPCGxVvS2um7UJlSgfSoPMklfgsADTW/wmPqGGH
5wIDAQAB
-----END PUBLIC KEY-----`,
		}, {
			name: "token 4096",
			config: &generator.Config{
				PrivateKey: `-----BEGIN RSA PRIVATE KEY-----
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
-----END RSA PRIVATE KEY-----`,
				Payload: generator.Payload{
					Issuer:   "user",
					Audience: "public",
				},
			},
			public: `-----BEGIN PUBLIC KEY-----
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
			config: &generator.Config{
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
				Payload: generator.Payload{
					Issuer:   "user",
					Audience: "public",
				},
			},
			public: `-----BEGIN PUBLIC KEY-----
MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAJBr38K42gXp+GslyYaLsDw5PM9x5Gbt
i5YpejFOIRhb9BCJUTGGg6NHv6AAWoJZ8jDWwa0IYhCaTYA1G+1emq8CAwEAAQ==
-----END PUBLIC KEY-----`,
		}, {
			name: "openssl 4096",
			config: &generator.Config{
				Password: "test",
				// Instructions to create:
				// Generate the private key using (provide a password): openssl genrsa -des3 -out private.pem 4096
				// Generate the corresponding public key (use the password): openssl rsa -in private.pem -outform PEM -pubout -out public.pem
				PrivateKey: `-----BEGIN RSA PRIVATE KEY-----
Proc-Type: 4,ENCRYPTED
DEK-Info: DES-EDE3-CBC,98EB424CDBFE3603

+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
-----END RSA PRIVATE KEY-----`,
				Payload: generator.Payload{
					Issuer:   "user",
					Audience: "public",
				},
			},
			public: `-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAsFLrSrMVZ1mLSELlcHuZ
gtSEHwR6+e+IV1matniYQWyifffOiyQUmMBYGkCdJ/kQpop0FEGi06KhTkQ0iHnK
LhG2NYgXXCM53wH3W496h86wHh0v6rlQe/fO0db08JO60VX6Eu0TWJ1JPXt8sS33
T6HGf1dTf1n3Aoy63cyfXwxLHW9LyQ6HwZKwEVHPEbUPSdkmOPqU23DrMf+u0Ftk
hjr32DgIWLB+QWXCyji3qApHwhTnqbOdfwCde2sQNtv4DihFeXyQu1VZrGgbgHTI
Q70qdMyDj53jXnBcUWIkQv9DFIQUyockTUChrW40+mQ+5ujKi4PgZ0zf6vRuIfD9
ttlwRPJ+K9AafEYFWd/FHKrTk9nG3MooejnuLTAGkm2i2cr+CvsODLPGhAwEBe7P
fssqqFNBYhWN/IiBxUB1AL4omMzU8V7aRs+aldKpNQMu8HDFIuEhVzz2EzA64q1W
vxaSonqnEdKBDTxj/85T3SJ1Ma7CM3tvbumjlp+V90sPp1o74b7Gt9IUrwzGkxvW
9EGFlANW/r/PXY9LUn7Nm9ZksQKndLP0Dbd+fUKZ4f1UyXMowa7FtCN6Kk1dsb4F
va4rrLdEv+FvwJJWwbxiAaDyfSMYvINX9mowWYuGo1ks4ldKCcTZoVitH1aAYP5x
grRQrd/wzHvpUTH0YtarWfcCAwEAAQ==
-----END PUBLIC KEY-----`,
		}, {
			name: "openssl 512",
			config: &generator.Config{
				Password: "test",
				// Instructions to create:
				// Generate the private key using (provide a password): openssl genrsa -des3 -out private.pem 512
				// Generate the corresponding public key (use the password): openssl rsa -in private.pem -outform PEM -pubout -out public.pem
				PrivateKey: `-----BEGIN RSA PRIVATE KEY-----
Proc-Type: 4,ENCRYPTED
DEK-Info: DES-EDE3-CBC,F1063F977F55EDFA

aDKltLnbCKYilHk74yzkGycL/Yc1mHzxfgeupNMHygXrji+UzwY1gjngzKwpN7IG
GU0a1jJVWo8RcKArSOPVvBEmMqzGBWvg19a0QNrlr8lMvkyQzmVsE0pMYCA5vE95
kFTzNVRRMGJ8JiNjFNVPB9h5dq46JP7gyBa/gu6G7btEmVPgCMW47mkMCThkfFsK
gObAYoR42UVxUkPdSfCveUL6e5FeJ7XVI15HctFp60F1D3BgglhXXgvyUDpY4b/Z
fwxp4VOfUTfR0TxC5i8AyQJ9B0WAdIL2gEFbuZpy1CBZhmUW75AD26WjIg41oky6
762TXXRitqgHiOwt+EGJ0lgdzok0qC4EEPzY37WyFhOKRr8VJXmxIPj+XQbiBbwM
WXZssMSlS6exxgle1sSESw8gzQ9ybQfrZs39fVlW80w=
-----END RSA PRIVATE KEY-----`,
				Payload: generator.Payload{
					Issuer:   "user",
					Audience: "public",
				},
			},
			public: `-----BEGIN PUBLIC KEY-----
MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAOKQ0CoAqGjbzXqqCtoPUYqaHxEVBMmF
08xYfxkIxxa/Pr4jxI39gbBBu6xznLfh2i7ThnDK7MK0YNvyI0/nV48CAwEAAQ==
-----END PUBLIC KEY-----`,
		}, {
			name: "openssl 512",
			config: &generator.Config{
				Password: "test",
				// Instructions to create:
				// Generate the private key using (provide a password): openssl genrsa -des3 -out private.pem 512
				// Generate the corresponding public key (use the password): openssl rsa -in private.pem -outform PEM -pubout -out public.pem
				PrivateKey: `-----BEGIN RSA PRIVATE KEY-----
Proc-Type: 4,ENCRYPTED
DEK-Info: DES-EDE3-CBC,F1063F977F55EDFA

aDKltLnbCKYilHk74yzkGycL/Yc1mHzxfgeupNMHygXrji+UzwY1gjngzKwpN7IG
GU0a1jJVWo8RcKArSOPVvBEmMqzGBWvg19a0QNrlr8lMvkyQzmVsE0pMYCA5vE95
kFTzNVRRMGJ8JiNjFNVPB9h5dq46JP7gyBa/gu6G7btEmVPgCMW47mkMCThkfFsK
gObAYoR42UVxUkPdSfCveUL6e5FeJ7XVI15HctFp60F1D3BgglhXXgvyUDpY4b/Z
fwxp4VOfUTfR0TxC5i8AyQJ9B0WAdIL2gEFbuZpy1CBZhmUW75AD26WjIg41oky6
762TXXRitqgHiOwt+EGJ0lgdzok0qC4EEPzY37WyFhOKRr8VJXmxIPj+XQbiBbwM
WXZssMSlS6exxgle1sSESw8gzQ9ybQfrZs39fVlW80w=
-----END RSA PRIVATE KEY-----`,
				Payload: generator.Payload{
					Issuer:   "user",
					Audience: "public",
				},
			},
			public: `-----BEGIN PUBLIC KEY-----
MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAOKQ0CoAqGjbzXqqCtoPUYqaHxEVBMmF
08xYfxkIxxa/Pr4jxI39gbBBu6xznLfh2i7ThnDK7MK0YNvyI0/nV48CAwEAAQ==
-----END PUBLIC KEY-----`,
		}, {
			name: "file 512",
			config: &generator.Config{
				Password: "test",
				// Instructions to create:
				// Generate the private key using (provide a password): openssl genrsa -des3 -out private.pem 512
				// Generate the corresponding public key (use the password): openssl rsa -in private.pem -outform PEM -pubout -out public.pem
				PrivateKeyPath: "test-keys/private-512.pem",
				Payload: generator.Payload{
					Issuer:   "user",
					Audience: "public",
				},
			},
			public:       "test-keys/public-512.pem",
			isPublicFile: true,
		}, {
			name: "file 1024",
			config: &generator.Config{
				Password: "test",
				// Instructions to create:
				// Generate the private key using (provide a password): openssl genrsa -des3 -out private.pem 512
				// Generate the corresponding public key (use the password): openssl rsa -in private.pem -outform PEM -pubout -out public.pem
				PrivateKeyPath: "test-keys/private-1024.pem",
				Payload: generator.Payload{
					Issuer:   "user",
					Audience: "public",
				},
			},
			public:       "test-keys/public-1024.pem",
			isPublicFile: true,
		}, {
			name: "file 2048",
			config: &generator.Config{
				Password: "test",
				// Instructions to create:
				// Generate the private key using (provide a password): openssl genrsa -des3 -out private.pem 512
				// Generate the corresponding public key (use the password): openssl rsa -in private.pem -outform PEM -pubout -out public.pem
				PrivateKeyPath: "test-keys/private-2048.pem",
				Payload: generator.Payload{
					Issuer:   "user",
					Audience: "public",
				},
			},
			public:       "test-keys/public-2048.pem",
			isPublicFile: true,
		}, {
			name: "file 4096",
			config: &generator.Config{
				Password: "test",
				// Instructions to create:
				// Generate the private key using (provide a password): openssl genrsa -des3 -out private.pem 512
				// Generate the corresponding public key (use the password): openssl rsa -in private.pem -outform PEM -pubout -out public.pem
				PrivateKeyPath: "test-keys/private-4096.pem",
				Payload: generator.Payload{
					Issuer:   "user",
					Audience: "public",
				},
			},
			public:       "test-keys/public-4096.pem",
			isPublicFile: true,
		},
	}
	for _, tc := range tcs {
		if tc.config.PrivateKeyPath != "" {
			require.NoError(t, tc.config.Validate(), tc.name)
			require.NoError(t, tc.config.FillKeyFromFiles(), tc.name)
		}
		token, err := generator.GenerateToken(tc.config)
		require.NoErrorf(t, err, tc.name)

		verifierConfig := &verifier.Config{
			Payload: verifier.Payload{
				Issuer:   tc.config.Issuer,
				Audience: tc.config.Audience,
			},
		}
		if tc.isPublicFile {
			verifierConfig.PublicKeyPath = tc.public
		} else {
			verifierConfig.PublicKey = tc.public
		}

		if verifierConfig.PublicKeyPath != "" {
			require.NoError(t, verifierConfig.Validate(), tc.name)
			require.NoError(t, verifierConfig.FillKeyFromFiles(), tc.name)
		}

		ok, err := verifier.VerifyToken(verifierConfig, token, tc.config.Audience)
		require.NoError(t, err, tc.name)
		if !ok {
			require.Fail(t, "verification failed", tc.name)
		}
	}
}

func TestAuth0Token(t *testing.T) {
	claims := jwt_claims.Claims{}
	// publicKey is extracted from the domain. Right now its hard coded as we have limit of 1000 requests only.
	publicKey := `MIIDDTCCAfWgAwIBAgIJXd8numgtiI74MA0GCSqGSIb3DQEBCwUAMCQxIjAgBgNVBAMTGWRldi1jZm9veTZpdS51cy5hdXRoMC5jb20wHhcNMjEwMzA4MDQ1ODE4WhcNMzQxMTE1MDQ1ODE4WjAkMSIwIAYDVQQDExlkZXYtY2Zvb3k2aXUudXMuYXV0aDAuY29tMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA068FNhugFSCLLHUHZvXw9DLPXxifdVIQa44yNMBnNGb6ZpfwDrLZtnP7Gd3B7bQthbpeBX722`
	tokenString := `eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IlRaUkZfVVNhaUs1amFKaUFDZm04aCJ9.eyJpc3MiOiJodHRwczovL2Rldi1jZm9veTZpdS51cy5hdXRoMC5jb20vIiwic3ViIjoiRW54ZkV3Y01QWUIzS2lHZHc1STNiNlJXRG5abDd0aWtAY2xpZW50cyIsImF1ZCI6Imh0dHBzOi8vcXVpY2tzdGFydHMvYXBpIiwiaWF0IjoxNjE1MTg4MjAxLCJleHAiOjE2MTUyNzQ2MDEsImF6cCI6IkVueGZFd2NNUFlCM0tpR2R3NUkzYjZSV0RuWmw3dGlrIiwiZ3R5IjoiY2xpZW50LWNyZWRlbnRpYWxzIn0.n1BK3UewqYiFzAR8HiQnuulzuXNtelI7HxRI_DtTn7ktj-ZTnb1B2ji1Bj_gwbqgSULuXU_JAq87el2aL7_Kwcvlbr7ZGwdAwi0Me0DK2v4FQ2niVuAt3FusaUvg8j93-WBqu9fAIpw6OXT9jgpcnH_KRgKM3n2K_X444WbFpDTj6mepBRY8wDQ0Kl4Y79iiAXoehDJfDrkkNT2X9K8LwDJOm7jfpYadrP267ykAI3k6epHZccnNqo0ZnUhXF_LDHoWS4ZeOHMPc0swDuG-4-Uv_qrqIyym4U7enutujUJmr7KBIChzZx-BR702WYQMnecxHOJ7kwthEVC8NX0EH8Q`
	tokenT, _ := jwt.ParseWithClaims(tokenString, &claims, func(token *jwt.Token) (interface{}, error) {
		result, err := jwt.ParseRSAPublicKeyFromPEM([]byte(publicKey))
		if err != nil {
			return nil, fmt.Errorf("claims validation: %w", err)
		}
		return result, nil
	}, jwt.WithAudience("https://quickstarts/api"))
	if !tokenT.Valid {
		require.Error(t, fmt.Errorf("token not valid"))
	}
}
