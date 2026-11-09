use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::manager::biz::NotificationBiz;
use crate::manager::validate;
use crate::pb::service::notification::{
    notification_service_server::NotificationService, AdminBroadcastRef,
    AdminCreateBroadcastRequest, AdminListBroadcastsRequest, AdminListBroadcastsResponse,
    AdminUpdateBroadcastRequest, ArchiveRequest, Broadcast, GetPreferencesRequest,
    GetUnreadCountRequest, ListActiveBannersRequest, ListActiveBannersResponse, ListInboxRequest,
    ListInboxResponse, MarkAllReadRequest, MarkReadRequest, Preferences, PublishRequest,
    PublishResponse, SetBudgetMuteRequest, UnreadCount, UpdatePreferencesRequest,
};

pub struct NotificationHandler {
    biz: Arc<NotificationBiz>,
}

impl NotificationHandler {
    pub fn new(biz: Arc<NotificationBiz>) -> Self {
        Self { biz }
    }
}

fn missing(what: &str) -> Status {
    Status::invalid_argument(format!("{what} is required"))
}

#[tonic::async_trait]
impl NotificationService for NotificationHandler {
    async fn list_inbox(
        &self,
        request: Request<ListInboxRequest>,
    ) -> Result<Response<ListInboxResponse>, Status> {
        let user_id = validate::normal_user(request.metadata())?;
        let resp = self.biz.list_inbox(&user_id, request.into_inner()).await?;
        Ok(Response::new(resp))
    }

    async fn get_unread_count(
        &self,
        request: Request<GetUnreadCountRequest>,
    ) -> Result<Response<UnreadCount>, Status> {
        let user_id = validate::normal_user(request.metadata())?;
        Ok(Response::new(self.biz.unread_count(&user_id).await?))
    }

    async fn mark_read(&self, request: Request<MarkReadRequest>) -> Result<Response<()>, Status> {
        let user_id = validate::normal_user(request.metadata())?;
        self.biz.mark_read(&user_id, request.into_inner()).await?;
        Ok(Response::new(()))
    }

    async fn mark_all_read(
        &self,
        request: Request<MarkAllReadRequest>,
    ) -> Result<Response<()>, Status> {
        let user_id = validate::normal_user(request.metadata())?;
        self.biz
            .mark_all_read(&user_id, request.into_inner())
            .await?;
        Ok(Response::new(()))
    }

    async fn archive(&self, request: Request<ArchiveRequest>) -> Result<Response<()>, Status> {
        let user_id = validate::normal_user(request.metadata())?;
        self.biz.archive(&user_id, request.into_inner()).await?;
        Ok(Response::new(()))
    }

    async fn list_active_banners(
        &self,
        request: Request<ListActiveBannersRequest>,
    ) -> Result<Response<ListActiveBannersResponse>, Status> {
        let user_id = validate::normal_user(request.metadata())?;
        let req = request.into_inner();
        let banners = self.biz.active_banners(&user_id, &req.locale).await?;
        Ok(Response::new(ListActiveBannersResponse { banners }))
    }

    async fn get_preferences(
        &self,
        request: Request<GetPreferencesRequest>,
    ) -> Result<Response<Preferences>, Status> {
        let user_id = validate::normal_user(request.metadata())?;
        Ok(Response::new(self.biz.preferences(&user_id).await?))
    }

    async fn update_preferences(
        &self,
        request: Request<UpdatePreferencesRequest>,
    ) -> Result<Response<Preferences>, Status> {
        let user_id = validate::normal_user(request.metadata())?;
        let resp = self
            .biz
            .update_preferences(&user_id, request.into_inner())
            .await?;
        Ok(Response::new(resp))
    }

    async fn set_budget_mute(
        &self,
        request: Request<SetBudgetMuteRequest>,
    ) -> Result<Response<Preferences>, Status> {
        let user_id = validate::normal_user(request.metadata())?;
        let req = request.into_inner();
        let resp = self
            .biz
            .set_budget_mute(&user_id, &req.budget_id, req.muted)
            .await?;
        Ok(Response::new(resp))
    }

    async fn admin_create_broadcast(
        &self,
        request: Request<AdminCreateBroadcastRequest>,
    ) -> Result<Response<Broadcast>, Status> {
        let admin_id = validate::super_admin(request.metadata())?;
        let input = request.into_inner().input.ok_or_else(|| missing("input"))?;
        Ok(Response::new(
            self.biz.admin_create_broadcast(&admin_id, input).await?,
        ))
    }

    async fn admin_update_broadcast(
        &self,
        request: Request<AdminUpdateBroadcastRequest>,
    ) -> Result<Response<Broadcast>, Status> {
        validate::super_admin(request.metadata())?;
        let req = request.into_inner();
        let input = req.input.ok_or_else(|| missing("input"))?;
        Ok(Response::new(
            self.biz
                .admin_update_broadcast(&req.broadcast_id, input)
                .await?,
        ))
    }

    async fn admin_publish_broadcast(
        &self,
        request: Request<AdminBroadcastRef>,
    ) -> Result<Response<Broadcast>, Status> {
        validate::super_admin(request.metadata())?;
        let id = request.into_inner().broadcast_id;
        Ok(Response::new(self.biz.admin_publish_broadcast(&id).await?))
    }

    async fn admin_archive_broadcast(
        &self,
        request: Request<AdminBroadcastRef>,
    ) -> Result<Response<Broadcast>, Status> {
        validate::super_admin(request.metadata())?;
        let id = request.into_inner().broadcast_id;
        Ok(Response::new(self.biz.admin_archive_broadcast(&id).await?))
    }

    async fn admin_list_broadcasts(
        &self,
        request: Request<AdminListBroadcastsRequest>,
    ) -> Result<Response<AdminListBroadcastsResponse>, Status> {
        validate::super_admin(request.metadata())?;
        Ok(Response::new(
            self.biz.admin_list_broadcasts(request.into_inner()).await?,
        ))
    }

    async fn publish(
        &self,
        request: Request<PublishRequest>,
    ) -> Result<Response<PublishResponse>, Status> {
        // Internal only: the gateway never routes this RPC.
        self.biz.check_internal_token(request.metadata())?;
        Ok(Response::new(self.biz.publish(request.into_inner()).await?))
    }
}
