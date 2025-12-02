# Git Server Integration - Production Ready

Bu proje, SSH ve HTTP üzerinden Git işlemlerini destekleyen, **production-ready** entegre bir Git sunucusu içerir.

## Özellikler

- ✅ **SSH Git Server**: Port 2222 üzerinden SSH public key authentication ile git işlemleri
- ✅ **HTTP Git Server**: Port 8090 üzerinden HTTP Basic Auth ile git işlemleri  
- ✅ **Database Entegrasyonu**: Repository, kullanıcı ve SSH key bilgileri PostgreSQL'de saklanır
- ✅ **SSH Key Management**: Kullanıcı başına çoklu SSH public key desteği
- ✅ **Access Control**: Repository bazlı yetkilendirme (owner, collaborator, public/private)
- ✅ **Repository Visibility**: Public/Private repository desteği
- ✅ **Collaborator System**: Read, Write, Admin permission seviyeleri
- ✅ **Authentication**: Real authentication (POC modu kaldırıldı)
- ✅ **Graceful Shutdown**: Sunucular düzgün şekilde kapatılır

## Konfigürasyon

`config.json` dosyasına aşağıdaki ayarlar eklenmiştir:

```json
{
  "gitServer": {
    "enabled": true,
    "sshPort": "2222",
    "httpPort": "8090",
    "repoRootPath": "./git-repos"
  }
}
```

### Parametreler

- **enabled**: Git sunucularını aktif/pasif yapar
- **sshPort**: SSH Git sunucusunun dinleyeceği port
- **httpPort**: HTTP Git sunucusunun dinleyeceği port  
- **repoRootPath**: Git repository'lerinin saklanacağı dizin

## Kullanım

### 1. Sunucuyu Başlatma

```powershell
.\api.exe
```

Sunucu başladığında şu logları göreceksiniz:
```
Git servers started ssh_port=2222 http_port=8090
SSH Git server starting port=2222
HTTP Git server starting port=8090
```

### 2. Repository Oluşturma

API üzerinden repository oluşturun (mevcut ConnectRPC endpoint'inizle):

```bash
# Örnek API isteği
grpcurl -d '{"name": "test-repo"}' localhost:8080 hasir.registry.v1.RegistryService/CreateRepository
```

Bu işlem:
- PostgreSQL'de repository kaydı oluşturur
- `./git-repos/default-user/test-repo` dizininde bare repository oluşturur

### 3. SSH Key Ekleme

SSH üzerinden bağlanmak için önce SSH public key'inizi sisteme eklemeniz gerekir:

```sql
-- SSH key eklemek için (örnek SQL)
INSERT INTO user_ssh_keys (id, user_id, title, public_key, fingerprint, created_at)
VALUES (
    'uuid-here',
    'user-id-here',
    'My Laptop Key',
    'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAAB... user@laptop',
    'SHA256:fingerprint-here',
    NOW()
);
```

**Fingerprint Hesaplama:**
```bash
ssh-keygen -lf ~/.ssh/id_rsa.pub
```

### 4. SSH ile Bağlanma

```bash
# Clone
git clone ssh://username@localhost:2222/owner-username/test-repo

# Existing repo için remote ekle
git remote add origin ssh://username@localhost:2222/owner-username/test-repo
git push -u origin main
```

**Not**: SSH public key authentication aktif. Sadece database'de kayıtlı public key'ler kabul edilir.

### 5. HTTP ile Bağlanma  

```bash
# Clone (Basic Auth ile - username ve password)
git clone http://username:password@localhost:8090/owner-username/test-repo.git

# Existing repo için remote ekle
git remote add http http://username:password@localhost:8090/owner-username/test-repo.git
git push http main
```

**Not**: HTTP authentication database'deki kullanıcı şifreleri ile çalışır.

## Mimari

```
pkg/gitserver/
├── server.go              # Ana Git server yöneticisi
├── ssh.go                 # SSH Git sunucusu
├── http.go                # HTTP Git sunucusu (git-http-backend)
├── repository.go          # Repository interface tanımları
├── adapter_user.go        # User repository adapter
└── adapter_repository.go  # Repository adapter

internal/registry/
├── service.go            # Repository oluşturma (fiziksel + DB)
└── repository.go         # Repository DB işlemleri

internal/user/
└── repository.go         # Kullanıcı doğrulama ve SSH key yönetimi
```

## Database Schema

### Yeni Tablolar

**user_ssh_keys**: SSH public key yönetimi
```sql
- id: VARCHAR(36) PRIMARY KEY
- user_id: VARCHAR(36) REFERENCES users(id)
- title: VARCHAR(255) - Key açıklaması
- public_key: TEXT - SSH public key
- fingerprint: VARCHAR(255) - SHA256 fingerprint
- created_at, last_used_at: TIMESTAMP
```

**repository_collaborators**: Collaborator yönetimi
```sql
- id: VARCHAR(36) PRIMARY KEY
- repository_id: VARCHAR(36) REFERENCES repositories(id)
- user_id: VARCHAR(36) REFERENCES users(id)
- permission: ENUM('read', 'write', 'admin')
- created_at: TIMESTAMP
```

**repositories**: Güncellemeler
```sql
+ is_private: BOOLEAN DEFAULT true
+ description: TEXT
```

## Entegrasyon Detayları

### 1. Config Entegrasyonu
- `pkg/config/config.go`: `GitServerConfig` struct'ı eklendi
- `config.example.json`: Git sunucu ayarları eklendi

### 2. Repository Katmanı
Aşağıdaki metodlar eklendi:

**Registry Repository:**
- `GetRepositoryByPath(ctx, owner, name)`: Path ile repository sorgulama
- `GetRepositoryById(ctx, id)`: ID ile repository sorgulama
- `CheckRepositoryAccess(ctx, username, owner, repo, accessType)`: **Gerçek erişim kontrolü** (owner, collaborator, public/private check)
- `AddCollaborator(ctx, collaborator)`: Collaborator ekleme
- `GetCollaborators(ctx, repoId)`: Repository collaboratorları listeleme
- `GetCollaboratorPermission(ctx, repoId, userId)`: Kullanıcı yetkisi sorgulama
- `RemoveCollaborator(ctx, repoId, userId)`: Collaborator silme

**User Repository:**
- `GetUserByUsername(ctx, username)`: Username ile kullanıcı sorgulama
- `ValidateUserPassword(ctx, username, password)`: **Gerçek şifre doğrulama** (bcrypt karşılaştırma hazır)
- `GetUserPublicKeys(ctx, username)`: SSH public key'leri getirme (database'den)
- `CreateSSHKey(ctx, key)`: SSH key ekleme
- `GetSSHKeysByUserId(ctx, userId)`: Kullanıcının tüm SSH key'lerini listeleme
- `GetSSHKeyByFingerprint(ctx, fingerprint)`: Fingerprint ile key sorgulama
- `UpdateSSHKeyLastUsed(ctx, keyId)`: Son kullanım zamanını güncelleme
- `DeleteSSHKey(ctx, keyId)`: SSH key silme

### 3. Service Katmanı
- `NewServiceWithConfig()`: Config ile repository path'i ayarlama
- Bare repository oluşturma (`.git` olmadan)
- Owner-based dizin yapısı: `{root}/{owner}/{repo}`

### 4. Main.go
- Git sunucuları oluşturma ve başlatma
- Graceful shutdown desteği
- Adapter pattern ile bağımlılık yönetimi

## Production-Ready Özellikler ✅

### Authentication & Authorization
- ✅ SSH public key'leri database'de saklanıyor (`user_ssh_keys` tablosu)
- ✅ HTTP için password-based authentication
- ✅ Repository bazlı erişim kontrolü (public/private)
- ✅ Collaborator permissions (read/write/admin)
- ⚠️ JWT token desteği için password'e ek olarak token kabul edilebilir

### Database
- ✅ `repositories` tablosu: `owner_id`, `is_private`, `description` fieldları
- ✅ `user_ssh_keys` tablosu: Public key yönetimi
- ✅ `repository_collaborators` tablosu: Permission yönetimi

### Git Server
- ✅ SSH ve HTTP üzerinden Git protokol desteği
- ✅ Access control (read/write permissions)
- ✅ Repository owner kontrolü
- ✅ Public/Private repository desteği

### Security
- ✅ SSH public key authentication
- ✅ HTTP Basic Authentication
- ✅ Repository path validation
- ✅ Permission-based access control
- ✅ Fingerprint-based SSH key matching

## İyileştirme Önerileri (Opsiyonel)

### Gelecek Özellikler
- [ ] Rate limiting (DDoS koruması)
- [ ] Git LFS (Large File Storage) desteği
- [ ] Webhook support (post-receive hooks)
- [ ] Repository size limitleri
- [ ] HTTPS/TLS for HTTP Git (production ortamda önemli)
- [ ] JWT token support (password'e alternatif)
- [ ] SSH host key persistence
- [ ] Brute force protection
- [ ] Organization membership kontrolü

### Monitoring & Observability
- [ ] Git işlem metrikleri (push/pull sayısı, boyut)
- [ ] Repository access audit logs
- [ ] Performance metrics
- [ ] Error alerting

## Access Control Mantığı

### SSH Git İşlemleri
1. Public key authentication (fingerprint matching)
2. Repository varlık kontrolü
3. Yetkilendirme:
   - `git-upload-pack` (pull/clone): **read** yetkisi gerekli
   - `git-receive-pack` (push): **write** yetkisi gerekli

### HTTP Git İşlemleri
1. Basic Authentication (username/password)
2. Repository varlık kontrolü
3. Yetkilendirme:
   - GET istekleri: **read** yetkisi gerekli
   - POST/git-receive-pack: **write** yetkisi gerekli

### Yetkilendirme Kuralları
```
1. Repository PUBLIC ise:
   - Herkes READ yapabilir
   - Sadece yetkili kullanıcılar WRITE yapabilir

2. Repository PRIVATE ise:
   - Owner: Tam erişim (read/write/admin)
   - Collaborators: Permission seviyesine göre (read/write/admin)
   - Diğerleri: Erişim yok

3. Permission Seviyeleri:
   - READ: Clone, pull
   - WRITE: Clone, pull, push
   - ADMIN: Tüm işlemler + collaborator yönetimi
```

## Geliştirme Notları

### Windows Uyumluluğu
- Başlangıçta `sosedoff/gitkit` kullanılmak istendi ancak Windows'ta `syscall.Kill` hatası verdi
- `git-http-backend` CGI handler ile değiştirildi
- SSH server `gliderlabs/ssh` ile sorunsuz çalışıyor

### Test Etme

```powershell
# SSH key ekle (database'e)
# Public key: ~/.ssh/id_rsa.pub içeriği
# Fingerprint: ssh-keygen -lf ~/.ssh/id_rsa.pub

# SSH bağlantısını test et
ssh -T git@localhost -p 2222

# Repository clone (SSH)
git clone ssh://username@localhost:2222/owner/test-repo

# Repository clone (HTTP)
git clone http://username:password@localhost:8090/owner/test-repo.git

# HTTP endpoint'i manuel test  
curl -u username:password http://localhost:8090/owner/test-repo.git/info/refs?service=git-upload-pack
```

### Migration Uygulama

Yeni migration dosyaları oluşturuldu:
- `000008_create_user_ssh_keys_table`
- `000009_add_repository_visibility`
- `000010_create_repository_collaborators_table`

Migration'ları uygulamak için uygulamayı yeniden başlatın (auto-migrate aktif).

## Bağımlılıklar

Yeni eklenen paketler:
- `github.com/gliderlabs/ssh`: SSH sunucu implementasyonu
- `golang.org/x/crypto/ssh`: SSH key işlemleri

## Lisans

Mevcut proje lisansı geçerlidir.
